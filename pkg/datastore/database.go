// Copyright 2023 VMware, Inc.
// Licensed to VMware, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. VMware, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package datastore

/*
Library for interacting with a relational database management system.
Requires the following environment variables to be set:
  - DB_HOST
  - DB_PORT
  - DB_ADMIN_USERNAME
  - DB_ADMIN_PASSWORD
  - DB_NAME
  - SSL_MODE

Right now the library expects that the structs that need to be persisted have a flat structure only
with strings, booleans, and signed integers, with all fields exported (starting with an uppercase letter)
*/

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
)

const (
	DbConfigOrgId      = "multitenant.orgId"      // Name of Postgres run-time config. parameter that will store current user's org. ID
	DbConfigInstanceId = "multitenant.instanceId" // Name of Postgres run-time config. parameter that will store current session's instance ID
)

/*
Postgres-backed implementation of DataStore interface. By default, uses MetadataBasedAuthorizer for authentication & authorization.
*/
type relationalDb struct {
	sync.RWMutex
	authorizer  authorizer.Authorizer // Allows or cancels operations on the DB depending on user's org. and service roles
	instancer   authorizer.Instancer  // Allows multi instance services to persist data with separation
	gormDBMap   map[dbrole.DbRole]*gorm.DB
	logger      *logrus.Entry
	initializer func(db *relationalDb, dbRole dbrole.DbRole) error
}

type TenancyInfo struct {
	DbRole     dbrole.DbRole
	InstanceId string
	OrgId      string
}

func (db *relationalDb) TestHelper() TestHelper {
	return db
}

func (db *relationalDb) Helper() Helper {
	return db
}

func (db *relationalDb) GetDBTransaction(ctx context.Context, tableName string, record Record) (tx *gorm.DB, err error) {
	if !IsPointerToStruct(record) {
		return nil, ErrNotPtrToStruct.WithValue(TYPE, TypeName(record))
	}
	return db.getDBTransaction(ctx, tableName, record)
}

func (db *relationalDb) getDBTransaction(ctx context.Context, tableName string, record Record) (tx *gorm.DB, err error) {
	var tenancyInfo TenancyInfo
	if err, tenancyInfo = db.getTenancyInfoFromCtx(ctx, tableName); err != nil {
		return nil, err
	}

	if err = db.ValidateTenancyScope(tenancyInfo, record, tableName); err != nil {
		return nil, err
	}

	if tx, err = db.configureTxWithTenancyScope(tenancyInfo); err != nil {
		return nil, err
	}

	tx = tx.Table(tableName)
	if err = tx.Error; err != nil {
		err = ErrStartingTx.Wrap(err).WithMap(map[ErrorContextKey]string{
			"db_role":    string(tenancyInfo.DbRole),
			"table_name": tableName,
			"authorizer": TypeName(db.authorizer),
		})
		db.logger.Error(err)
		return nil, err
	}
	return tx, nil
}

func (db *relationalDb) GetTransaction(ctx context.Context, records ...Record) (tx *gorm.DB, err error) {
	tableNames := make([]string, 0)
	for _, record := range records {
		tableNames = append(tableNames, GetTableName(record))
	}

	err, tenancyInfo := db.getTenancyInfoFromCtx(ctx, tableNames...)
	if err != nil {
		return nil, err
	}

	for i, record := range records {
		tableName := tableNames[i]
		err := db.ValidateTenancyScope(tenancyInfo, record, tableName)
		if err != nil {
			return nil, err
		}
	}
	return db.configureTxWithTenancyScope(tenancyInfo)
}

// If the DB role is tenant-specific (TENANT_READER or TENANT_WRITER) and the table is multi-tenant,
// make sure that the record being inserted/modified/updated/deleted/queried belongs to the user's org.
// If operation is SELECT but no specific tenant's data is being queried (e.g., FindAll() was called),
// allow the operation to proceed.
// If the table is multi-instanced, the record is expected to have the InstanceId properly configured, based on
// context.
func (db *relationalDb) ValidateTenancyScope(tenancyInfo TenancyInfo, record Record, tableName string) error {
	if tenancyInfo.DbRole.IsDbRoleTenantScoped() && IsMultiTenanted(record, tableName) {
		orgIdCol, _ := GetOrgId(record)
		if orgIdCol != "" && orgIdCol != tenancyInfo.OrgId {
			err := ErrOperationNotAllowed.WithValue("tenant", tenancyInfo.OrgId).WithValue("orgIdCol", orgIdCol)
			db.logger.Error(err)
			return err
		}
	}
	if IsMultiInstanced(record, tableName, db.instancer != nil) {
		instanceIdCol, _ := GetInstanceId(record)
		if instanceIdCol != "" && instanceIdCol != tenancyInfo.InstanceId {
			err := ErrOperationNotAllowed.WithValue("instance", tenancyInfo.InstanceId).WithValue("instanceIdCol", instanceIdCol)
			db.logger.Error(err)
			return err
		}
	}
	return nil
}

func (db *relationalDb) configureTxWithTenancyScope(tenancyInfo TenancyInfo) (*gorm.DB, error) {
	tx, err := db.GetDBConn(tenancyInfo.DbRole)
	if err != nil {
		return nil, err
	}
	tx = tx.Begin()
	if tenancyInfo.DbRole.IsDbRoleTenantScoped() {
		// Set org. ID
		stmt := getSetConfigStmt(DbConfigOrgId, tenancyInfo.OrgId)
		if err = tx.Exec(stmt).Error; err != nil {
			db.logger.Error(err)
			return nil, ErrExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmt)
		}
	} else {
		TRACE("Skipping tenant scoping for %+v", tenancyInfo)
	}

	// Set config view scoped to InstanceId
	stmt := getSetConfigStmt(DbConfigInstanceId, tenancyInfo.InstanceId)
	if err = tx.Exec(stmt).Error; err != nil {
		db.logger.Error(err)
		return nil, ErrExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmt)
	}
	return tx, nil
}

// Reset Resets DB connection pools.
func (db *relationalDb) Reset() {
	db.gormDBMap = make(map[dbrole.DbRole]*gorm.DB)
	db.authorizer = authorizer.MetadataBasedAuthorizer{}
}

// Find Finds a single record that has the same values as non-zero fields in the record.
// record argument must be a pointer to a struct and will be modified in-place.
// Returns ErrRecordNotFound if a record could not be found.
func (db *relationalDb) Find(ctx context.Context, record Record) error {
	return db.FindInTable(ctx, GetTableName(record), record)
}

func rollbackTx(tx *gorm.DB, db *relationalDb) {
	if err := tx.Rollback().Error; err != nil && err != sql.ErrTxDone {
		db.logger.Error(err)
	}
}

// Finds a single record that has the same values as non-zero fields in the record.
// record argument must be a pointer to a struct and will be modified in-place.
// Returns ErrRecordNotFound if a record could not be found.
func (db *relationalDb) FindInTable(ctx context.Context, tableName string, record Record) (err error) {
	var tx *gorm.DB
	if tx, err = db.GetDBTransaction(ctx, tableName, record); err != nil {
		return err
	}
	defer rollbackTx(tx, db)

	if err = tx.Table(tableName).Where(record).First(record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrRecordNotFound.Wrap(err).WithValue("record", fmt.Sprintf("%+v", record))
		}
		db.logger.Error(err)
		return ErrExecutingSqlStmt.Wrap(err)
	}
	if err = tx.Commit().Error; err != nil {
		db.logger.Error(err)
		return ErrExecutingSqlStmt.Wrap(err)
	}
	return nil
}

// Finds all records in a DB table.
// records must be a pointer to a slice of structs and will be modified in-place.
func (db *relationalDb) FindAll(ctx context.Context, records interface{}, pagination *Pagination) error {
	if reflect.TypeOf(records).Kind() != reflect.Ptr || reflect.TypeOf(records).Elem().Kind() != reflect.Slice {
		errMsg := "\"records\" argument has to be a pointer to a slice of structs implementing \"Record\" interface"
		err := ErrNotPtrToStructSlice.Wrap(fmt.Errorf(errMsg))
		db.logger.Error(err)
		return err
	}

	tableName := GetTableName(records)
	return db.FindAllInTable(ctx, tableName, records, pagination)
}

// FindAllInTable Finds all records in DB table tableName.
// records must be a pointer to a slice of structs and will be modified in-place.
func (db *relationalDb) FindAllInTable(ctx context.Context, tableName string, records interface{}, pagination *Pagination) error {
	record := GetRecordInstanceFromSlice(records)
	return db.FindWithFilterInTable(ctx, tableName, record, records, pagination)
}

// FindWithFilter Finds multiple records in a DB table.
// If record argument is non-empty, uses the non-empty fields as criteria in a query.
// records must be a pointer to a slice of structs and will be modified in-place.
func (db *relationalDb) FindWithFilter(ctx context.Context, record Record, records interface{}, pagination *Pagination) error {
	return db.FindWithFilterInTable(ctx, GetTableName(record), record, records, pagination)
}

// Finds multiple records in DB table tableName.
// If record argument is non-empty, uses the non-empty fields as criteria in a query.
// records must be a pointer to a slice of structs and will be modified in-place.
func (db *relationalDb) FindWithFilterInTable(ctx context.Context, tableName string, record Record, records interface{}, pagination *Pagination) (err error) {
	if reflect.TypeOf(records).Kind() != reflect.Ptr || reflect.TypeOf(records).Elem().Kind() != reflect.Slice {
		return ErrNotPtrToStruct.WithValue(TYPE, TypeName(records))
	}

	var tx *gorm.DB
	if tx, err = db.getDBTransaction(ctx, tableName, record); err != nil {
		return err
	}

	if err = tx.Table(tableName).Where(record).Error; err != nil {
		db.logger.Error(err)
		return ErrExecutingSqlStmt.Wrap(err)
	}
	if pagination != nil {
		tx.Offset(pagination.Offset).Limit(pagination.Limit)
		if pagination.SortBy != "" {
			tx.Order(pagination.SortBy)
		}
	}
	if err = tx.Find(records).Error; err != nil {
		db.logger.Error(err)
		return ErrExecutingSqlStmt.Wrap(err)
	}

	if err = tx.Commit().Error; err != nil {
		db.logger.Error(err)
		return ErrExecutingSqlStmt.Wrap(err)
	}
	return nil
}

/*
Inserts a record into a DB table.
*/
func (db *relationalDb) Insert(ctx context.Context, record Record) (rowsAffected int64, err error) {
	return db.InsertInTable(ctx, GetTableName(record), record)
}

/*
Inserts a record into DB table tableName.
*/
func (db *relationalDb) InsertInTable(ctx context.Context, tableName string, record Record) (rowsAffected int64, err error) {
	var tx *gorm.DB
	if tx, err = db.GetDBTransaction(ctx, tableName, record); err != nil {
		return 0, err
	}
	defer rollbackTx(tx, db)

	if err = tx.Create(record).Error; err != nil {
		db.logger.Error(err)
		return 0, ErrExecutingSqlStmt.Wrap(err)
	}
	if err = tx.Commit().Error; err != nil {
		db.logger.Error(err)
		return 0, ErrExecutingSqlStmt.Wrap(err)
	}
	return tx.RowsAffected, nil
}

/*
Deletes a record from a DB table, soft deletes if `DeletedAt` attribute is present.
*/
func (db *relationalDb) SoftDelete(ctx context.Context, record Record) (rowsAffected int64, err error) {
	return db.SoftDeleteInTable(ctx, GetTableName(record), record)
}

/*
Deletes a record from a DB table. (to be used after soft-delete for structs with `DeletedAt` field).
*/
func (db *relationalDb) Delete(ctx context.Context, record Record) (rowsAffected int64, err error) {
	return db.DeleteInTable(ctx, GetTableName(record), record)
}

/*
Deletes a record from DB table tableName, soft deletes if `DeletedAt` attribute is present.
*/
func (db *relationalDb) SoftDeleteInTable(ctx context.Context, tableName string, record Record) (rowsAffected int64, err error) {
	return db.delete(ctx, tableName, record, true)
}

/*
Deletes a record from DB table tableName. (to be used after soft-delete for structs with 'DeletedAt` fields).
*/
func (db *relationalDb) DeleteInTable(ctx context.Context, tableName string, record Record) (rowsAffected int64, err error) {
	return db.delete(ctx, tableName, record, false)
}

func (db *relationalDb) delete(ctx context.Context, tableName string, record Record, softDelete bool) (rowsAffected int64, err error) {
	tx, err := db.GetDBTransaction(ctx, tableName, record)
	if err != nil {
		return 0, err
	}
	defer rollbackTx(tx, db)

	if softDelete {
		if err = tx.Delete(record).Error; err != nil {
			db.logger.Error(err)
			return 0, ErrExecutingSqlStmt.Wrap(err)
		}
	} else {
		if err = tx.Unscoped().Delete(record).Error; err != nil {
			db.logger.Error(err)
			return 0, ErrExecutingSqlStmt.Wrap(err)
		}
	}
	if err = tx.Commit().Error; err != nil {
		db.logger.Error(err)
		return 0, ErrExecutingSqlStmt.Wrap(err)
	}
	return tx.RowsAffected, nil
}

/*
 * Drops the DB tables given by Records.
 */
func (db *relationalDb) DropTables(records ...Record) error {
	for _, record := range records {
		tx, err := db.GetDBConn(dbrole.MAIN)
		if err != nil {
			return err
		}
		err = tx.Migrator().DropTable(record)
		if err != nil {
			db.logger.Error(err)
			return ErrExecutingSqlStmt.Wrap(err)
		}
	}
	return nil
}

func (db *relationalDb) Truncate(tableNames ...string) error {
	return db.TruncateCascade(false, tableNames...)
}

func (db *relationalDb) TruncateCascade(cascade bool, tableNames ...string) (err error) {
	// Truncate DB tables
	var tx *gorm.DB
	for _, tableName := range tableNames {
		stmt := getTruncateTableStmt(tableName, cascade)
		if tx, err = db.GetDBConn(dbrole.MAIN); err != nil {
			return err
		}
		if err = tx.Exec(stmt).Error; err != nil {
			db.logger.Error(err)
			return ErrExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmt)
		}
	}

	return nil
}

/*
Updates a record in a DB table.
*/
func (db *relationalDb) Update(ctx context.Context, record Record) (rowsAffected int64, err error) {
	return db.UpdateInTable(ctx, GetTableName(record), record)
}

/*
Upserts a record in a DB table.
*/
func (db *relationalDb) Upsert(ctx context.Context, record Record) (rowsAffected int64, err error) {
	return db.UpsertInTable(ctx, GetTableName(record), record)
}

/*
Upserts a record in DB table tableName.
*/
func (db *relationalDb) UpsertInTable(ctx context.Context, tableName string, record Record) (rowsAffected int64, err error) {
	var tx *gorm.DB
	if tx, err = db.GetDBTransaction(ctx, tableName, record); err != nil {
		return 0, err
	}
	defer rollbackTx(tx, db)

	if err = tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(record).Error; err != nil {
		db.logger.Error(err)
		if strings.Contains(err.Error(), REVISION_OUTDATED_MSG) {
			return 0, ErrRevisionConflict.Wrap(err)
		} else {
			return 0, ErrExecutingSqlStmt.Wrap(err)
		}
	}
	if err = tx.Commit().Error; err != nil {
		db.logger.Error(err)
		return 0, ErrExecutingSqlStmt.Wrap(err)
	}
	return tx.RowsAffected, nil
}

/*
Updates a record in DB table tableName.
*/
func (db *relationalDb) UpdateInTable(ctx context.Context, tableName string, record Record) (rowsAffected int64, err error) {
	var tx *gorm.DB
	if tx, err = db.GetDBTransaction(ctx, tableName, record); err != nil {
		return 0, err
	}
	defer rollbackTx(tx, db)

	if err = tx.Model(record).Select("*").Updates(record).Error; err != nil {
		db.logger.Error(err)
		if strings.Contains(err.Error(), REVISION_OUTDATED_MSG) {
			return 0, ErrRevisionConflict.Wrap(err)
		} else {
			return 0, ErrExecutingSqlStmt.Wrap(err)
		}
	}
	if err = tx.Commit().Error; err != nil {
		db.logger.Error(err)
		return 0, ErrExecutingSqlStmt.Wrap(err)
	}
	return tx.RowsAffected, nil
}

// Registers a struct with DAL. See RegisterHelper() for more info.
func (db *relationalDb) Register(ctx context.Context, roleMapping map[string]dbrole.DbRole, records ...Record) error {
	for _, record := range records {
		err := db.RegisterHelper(ctx, roleMapping, GetTableName(record), record)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *relationalDb) RegisterHelper(_ context.Context, roleMapping map[string]dbrole.DbRole, tableName string, record Record) (err error) {
	if roleMapping == nil {
		roleMapping = make(map[string]dbrole.DbRole)
	}

	db.logger.Debugf("Registering the struct %q with DAL (backed by Postgres)... Using authorizer %s...", tableName, GetTableName(db.authorizer))

	tx, err := db.GetDBConn(dbrole.MAIN)
	if err != nil {
		return err
	}
	if err = tx.Table(tableName).AutoMigrate(record); err != nil {
		err = ErrRegisteringStruct.Wrap(err).WithValue(TABLE_NAME, tableName)
		db.logger.Error(err)
		return err
	}

	// Set up trigger on revision column for tables that need it
	if IsRevisioned(record, tableName) {
		if err = db.enforceRevisioning(tableName); err != nil {
			return err
		}
	}

	// Enable row-level security in a multi-tenant or multi-instanced table
	if IsRowLevelSecurityRequired(record, tableName, db.instancer != nil) {
		stmt := getEnableRLSStmt(tableName, record)
		tx, err = db.GetDBConn(dbrole.MAIN)
		if err != nil {
			return err
		}
		if err := tx.Exec(stmt).Error; err != nil {
			err = ErrRegisteringStruct.Wrap(err).WithMap(map[ErrorContextKey]string{
				TABLE_NAME: tableName,
				SQL_STMT:   stmt,
			})
			db.logger.Error(err)
			return err
		}
	}

	// Create users, grant privileges for current table, setup RLS-policies (if multi-tenant)
	users := getDbUsers(tableName, IsMultiTenanted(record, tableName), IsMultiInstanced(record, tableName, db.instancer != nil))
	for _, dbUserSpec := range users {
		if err = db.grantPrivileges(dbUserSpec, tableName, record); err != nil {
			err = ErrRegisteringStruct.Wrap(err).WithMap(map[ErrorContextKey]string{
				TABLE_NAME: tableName,
			})
			return err
		}
	}

	db.authorizer.Configure(tableName, roleMapping)
	return nil
}

/*
Creates a Postgres trigger that checks if a record being updated contains the most recent revision.
If not, update is rejected.
*/
func (db *relationalDb) enforceRevisioning(tableName string) (err error) {
	functionName, _ := getCheckAndUpdateRevisionFunc()

	var tx *gorm.DB
	if tx, err = db.GetDBConn(dbrole.MAIN); err != nil {
		return err
	}
	stmt := getDropTriggerStmt(tableName, functionName)
	if err = tx.Exec(stmt).Error; err != nil {
		err = ErrExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmt)
		db.logger.Error(err)
		return err
	}

	if tx, err = db.GetDBConn(dbrole.MAIN); err != nil {
		return err
	}
	stmt = getCreateTriggerStmt(tableName, functionName)
	if err = tx.Exec(stmt).Error; err != nil {
		if !strings.Contains(err.Error(), "duplicate key value") {
			db.logger.Error(err)
			return ErrExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmt)
		}
	}

	return nil
}

// Grants certain privileges to perform certain operations to the user (e.g., SELECT only; SELECT, INSERT, UPDATE, DELETE).
// Creates RLS-policy if the table is multi-tenant or multi-instanced.
func (db *relationalDb) grantPrivileges(dbUser dbUserSpec, tableName string, record Record) (err error) {
	tx, err := db.GetDBConn(dbrole.MAIN)
	if err != nil {
		return err
	}
	stmt := getGrantPrivilegesStmt(tableName, string(dbUser.username), dbUser.commands)
	if err := tx.Exec(stmt).Error; err != nil {
		db.logger.Error(err)
		return ErrExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmt)
	}

	// Enable row-level security in a multi-tenant or multi-instanced table
	if IsRowLevelSecurityRequired(record, tableName, db.instancer != nil) {
		tx, err = db.GetDBConn(dbrole.MAIN)
		if err != nil {
			return err
		}
		stmt = getCreatePolicyStmt(tableName, record, dbUser)
		if err := tx.Exec(stmt).Error; err != nil {
			err = ErrExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmt)
			db.logger.Error(err)
			return err
		}
	}

	return nil
}

func (db *relationalDb) GetAuthorizer() authorizer.Authorizer {
	return db.authorizer
}

func (db *relationalDb) GetInstancer() authorizer.Instancer {
	return db.instancer
}

// Uses an authorizer to get user's org. ID and a matching DB role.
// With the default MetadataBasedAuthorizer, does the following:
// Gets user's org ID and a DB role that matches one of its CSP roles.
// Returns an error if there are no role mappings for the given table, if user's org. ID cannot be retrieved from CSP,
// or if there is no matching DB role for any one of the user's CSP roles.
func (db *relationalDb) getTenancyInfoFromCtx(ctx context.Context, tableNames ...string) (err error, tenancyInfo TenancyInfo) {
	// Get the matching DB role
	tenancyInfo.DbRole, err = db.authorizer.GetMatchingDbRole(ctx, tableNames...)
	if err != nil {
		return err, tenancyInfo
	}

	tenancyInfo.OrgId, err = db.authorizer.GetOrgFromContext(ctx)
	if !tenancyInfo.DbRole.IsDbRoleTenantScoped() && errors.Is(err, ErrMissingOrgId) {
		err = nil
	}
	if err != nil {
		return err, tenancyInfo
	}
	if db.instancer != nil {
		tenancyInfo.InstanceId, err = db.instancer.GetInstanceId(ctx)
		if err != nil {
			TRACE("Skipping instance id for %+v", ctx)
			err = nil
		}
	}
	db.logger.Debugf("Tenancy Info from Context: %+v", tenancyInfo)
	return err, tenancyInfo
}

func (db *relationalDb) GetDBConn(dbRole dbrole.DbRole) (*gorm.DB, error) {
	if _, ok := db.gormDBMap[dbRole]; !ok {
		if err := db.initializer(db, dbRole); err != nil {
			err = ErrConnectingToDb.Wrap(err)
			return nil, err
		}
	}
	if conn, ok := db.gormDBMap[dbRole]; ok {
		TRACE("Returning DB connection for %s", dbRole)
		return conn, nil
	}
	return nil, ErrConnectingToDb.WithValue(DB_ROLE, string(dbRole))
}
