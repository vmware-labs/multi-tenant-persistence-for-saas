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
	"errors"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	DbConfigOrgId = "multitenant.orgId" // Name of Postgres run-time config. parameter that will store current user's org. ID
)

/*
Postgres-backed implementation of DataStore interface. By default, uses MetadataBasedAuthorizer for authentication & authorization.
*/
type relationalDb struct {
	authorizer  authorizer.Authorizer // Allows or cancels operations on the DB depending on user's org. and service roles
	gormDBMap   map[dbrole.DbRole]*gorm.DB
	logger      *logrus.Entry
	initializer func(db *relationalDb, dbRole dbrole.DbRole) error
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
	err, orgId, dbRole, isDbRoleTenantScoped := db.getTenantInfoFromCtx(ctx, tableName)
	if err != nil {
		return nil, err
	}

	// If the DB role is tenant-specific (TENANT_READER or TENANT_WRITER) and the table is multi-tenant,
	// make sure that the record being inserted/modified/updated/deleted/queried belongs to the user's org.
	// If operation is SELECT but no specific tenant's data is being queried (e.g., FindAll() was called),
	// allow the operation to proceed.
	if dbRole.IsDbRoleTenantScoped() && IsMultitenant(record, tableName) {
		orgIdCol, _ := GetOrgId(record)
		if orgIdCol != "" && orgIdCol != orgId {
			err = ErrOperationNotAllowed.WithValue("tenant", orgId).WithValue("orgIdCol", orgIdCol)
			db.logger.Error(err)
			return nil, err
		}
	}

	tx, err = db.GetDBConn(dbRole)
	if err != nil {
		return nil, err
	}
	tx = tx.Begin()
	if isDbRoleTenantScoped {
		// Set org. ID
		stmt := getSetConfigStmt(DbConfigOrgId, orgId)
		if tx = tx.Exec(stmt); tx.Error != nil {
			err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
			db.logger.Errorln(err)
			return nil, err
		}
	}
	tx = tx.Table(tableName)
	if tx.Error != nil {
		err = ErrStartingTx.Wrap(tx.Error).WithMap(map[ErrorContextKey]string{
			"db_role":    string(dbRole),
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

	err, orgId, dbRole, isDbRoleTenantScoped := db.getTenantInfoFromCtx(ctx, tableNames...)
	if err != nil {
		return nil, err
	}

	// If the DB role is tenant-specific (TENANT_READER or TENANT_WRITER) and the table is multi-tenant,
	// make sure that the record being inserted/modified/updated/deleted/queried belongs to the user's org.
	// If operation is SELECT but no specific tenant's data is being queried (e.g., FindAll() was called),
	// allow the operation to proceed.
	for _, record := range records {
		if dbRole.IsDbRoleTenantScoped() && IsMultitenant(record, GetTableName(record)) {
			orgIdCol, _ := GetOrgId(record)
			err = ErrOperationNotAllowed.WithValue("tenant", orgId).WithValue("orgIdCol", orgIdCol)
			db.logger.Error(err.Error())
			if orgIdCol != "" && orgIdCol != orgId {
				err = ErrOperationNotAllowed.WithValue("tenant", orgId).WithValue("orgIdCol", orgIdCol)
				db.logger.Error(err)
				return nil, err
			}
		}
	}

	tx, err = db.GetDBConn(dbRole)
	if err != nil {
		return nil, err
	}
	tx = tx.Begin()
	if isDbRoleTenantScoped {
		// Set org. ID
		stmt := getSetConfigStmt(DbConfigOrgId, orgId)
		if tx = tx.Exec(stmt); tx.Error != nil {
			err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
			db.logger.Errorln(err)
			return nil, err
		}
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

// Finds a single record that has the same values as non-zero fields in the record.
// record argument must be a pointer to a struct and will be modified in-place.
// Returns ErrRecordNotFound if a record could not be found.
func (db *relationalDb) FindInTable(ctx context.Context, tableName string, record Record) (err error) {
	tx, err := db.GetDBTransaction(ctx, tableName, record)
	if err != nil {
		return err
	}

	tx.Where(record).First(record)
	tx.Commit()
	if tx.RowsAffected == 0 {
		errMsg := fmt.Sprintf("Unable to locate a record: %+v", record)
		err = ErrRecordNotFound.Wrap(fmt.Errorf(errMsg))
		db.logger.Error(err)
		return err
	}
	if tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error)
		db.logger.Error(err)
		return err
	}
	return nil
}

// Finds all records in a DB table.
// records must be a pointer to a slice of structs and will be modified in-place.
func (db *relationalDb) FindAll(ctx context.Context, records interface{}) error {
	if reflect.TypeOf(records).Kind() != reflect.Ptr || reflect.TypeOf(records).Elem().Kind() != reflect.Slice {
		errMsg := "\"records\" argument has to be a pointer to a slice of structs implementing \"Record\" interface"
		err := ErrNotPtrToStructSlice.Wrap(fmt.Errorf(errMsg))
		db.logger.Error(err)
		return err
	}

	tableName := GetTableNameFromSlice(records)
	return db.FindAllInTable(ctx, tableName, records)
}

// FindAllInTable Finds all records in DB table tableName.
// records must be a pointer to a slice of structs and will be modified in-place.
func (db *relationalDb) FindAllInTable(ctx context.Context, tableName string, records interface{}) error {
	record := GetRecordInstanceFromSlice(records)
	return db.FindWithFilterInTable(ctx, tableName, record, records)
}

// FindWithFilter Finds multiple records in a DB table.
// If record argument is non-empty, uses the non-empty fields as criteria in a query.
// records must be a pointer to a slice of structs and will be modified in-place.
func (db *relationalDb) FindWithFilter(ctx context.Context, record Record, records interface{}) error {
	return db.FindWithFilterInTable(ctx, GetTableName(record), record, records)
}

// Finds multiple records in DB table tableName.
// If record argument is non-empty, uses the non-empty fields as criteria in a query.
// records must be a pointer to a slice of structs and will be modified in-place.
func (db *relationalDb) FindWithFilterInTable(ctx context.Context, tableName string, record Record, records interface{}) (err error) {
	if reflect.TypeOf(records).Kind() != reflect.Ptr || reflect.TypeOf(records).Elem().Kind() != reflect.Slice {
		return ErrNotPtrToStruct.WithValue(TYPE, TypeName(records))
	}

	tx, err := db.getDBTransaction(ctx, tableName, record)
	if err != nil {
		return err
	}

	tx.Where(record).Find(records) // FILTER by record too
	tx.Commit()
	if tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error)
		db.logger.Errorln(err)
		return err
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
	tx, err := db.GetDBTransaction(ctx, tableName, record)
	if err != nil {
		return 0, err
	}

	tx.Create(record)
	tx.Commit()
	if tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error)
		db.logger.Errorln(err)
		return 0, err
	}
	return tx.RowsAffected, nil
}

/*
Deletes a record from a DB table.
*/
func (db *relationalDb) Delete(ctx context.Context, record Record) (rowsAffected int64, err error) {
	return db.DeleteInTable(ctx, GetTableName(record), record)
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
			err = ErrExecutingSqlStmt.Wrap(err)
			db.logger.Errorln(err)
			return err
		}
	}
	return nil
}

func (db *relationalDb) Truncate(tableNames ...string) error {
	return db.TruncateCascade(false, tableNames...)
}

func (db *relationalDb) TruncateCascade(cascade bool, tableNames ...string) (err error) {
	// Truncate DB tables
	for _, tableName := range tableNames {
		stmt := getTruncateTableStmt(tableName, cascade)
		tx, err := db.GetDBConn(dbrole.MAIN)
		if err != nil {
			return err
		}
		if tx := tx.Exec(stmt); tx.Error != nil {
			err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
			db.logger.Errorln(err)
			return err
		}
	}

	return nil
}

/*
Deletes a record from DB table tableName.
*/
func (db *relationalDb) DeleteInTable(ctx context.Context, tableName string, record Record) (rowsAffected int64, err error) {
	tx, err := db.GetDBTransaction(ctx, tableName, record)
	if err != nil {
		return 0, err
	}

	// Execute query
	tx.Delete(record)
	tx.Commit()
	if tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error)
		db.logger.Errorln(err)
		return 0, err
	}
	return tx.RowsAffected, nil
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
	tx, err := db.GetDBTransaction(ctx, tableName, record)
	if err != nil {
		return 0, err
	}

	tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(record)
	tx.Commit()
	if tx.Error != nil {
		if strings.Contains(tx.Error.Error(), REVISION_OUTDATED_MSG) {
			err = ErrRevisionConflict.Wrap(tx.Error)
		} else {
			err = ErrExecutingSqlStmt.Wrap(tx.Error)
		}
		db.logger.Errorln(err)
		return 0, err
	}
	return tx.RowsAffected, nil
}

/*
Updates a record in DB table tableName.
*/
func (db *relationalDb) UpdateInTable(ctx context.Context, tableName string, record Record) (rowsAffected int64, err error) {
	tx, err := db.GetDBTransaction(ctx, tableName, record)
	if err != nil {
		return 0, err
	}

	tx.Model(record).Select("*").Updates(record)
	tx.Commit()
	if tx.Error != nil {
		if strings.Contains(tx.Error.Error(), REVISION_OUTDATED_MSG) {
			err = ErrRevisionConflict.Wrap(tx.Error)
		} else {
			err = ErrExecutingSqlStmt.Wrap(tx.Error)
		}
		db.logger.Errorln(err)
		return 0, err
	}
	return tx.RowsAffected, nil
}

// Registers a struct with DAL. See RegisterWithDALHelper() for more info.
func (db *relationalDb) RegisterWithDAL(ctx context.Context, roleMapping map[string]dbrole.DbRole, record Record) error {
	return db.RegisterWithDALHelper(ctx, roleMapping, GetTableName(record), record)
}

// Create a DB table for the given struct. Enables RLS in it if it is multi-tenant.
// Generates Postgres roles and policies based on the provided role mapping and applies them
// to the created DB table.
// roleMapping - maps service roles to DB roles to be used for the generated DB table
// There are 4 possible DB roles to choose from:
// - READER, which gives read access to all the records in the table
// - WRITER, which gives read & write access to all the records in the table
// - TENANT_READER, which gives read access to current tenant's records
// - TENANT_WRITER, which gives read & write access to current tenant's records.
func (db *relationalDb) RegisterWithDALHelper(_ context.Context, roleMapping map[string]dbrole.DbRole, tableName string, record Record) (err error) {
	if roleMapping == nil {
		roleMapping = make(map[string]dbrole.DbRole)
	}

	db.logger.Debugf("Registering the struct %q with DAL (backed by Postgres)... Using authorizer %s...", tableName, GetTableName(db.authorizer))

	tx, err := db.GetDBConn(dbrole.MAIN)
	if err != nil {
		return err
	}
	err = tx.Table(tableName).AutoMigrate(record)
	if err != nil {
		err = ErrRegisteringStruct.Wrap(err).WithValue(TABLE_NAME, tableName)
		db.logger.Error(err)
		return err
	}

	// Set up trigger on revision column for tables that need it
	if IsRevisioningSupported(tableName, record) {
		if err = db.enforceRevisioning(tableName); err != nil {
			return err
		}
	}

	// Enable row-level security in a multi-tenant table
	if IsMultitenant(record, tableName) {
		stmt := getEnableRLSStmt(tableName, record)
		if tx := db.gormDBMap[dbrole.MAIN].Exec(stmt); tx.Error != nil {
			err = ErrRegisteringStruct.Wrap(tx.Error).WithMap(map[ErrorContextKey]string{
				TABLE_NAME: tableName,
				SQL_STMT:   stmt,
			})
			db.logger.Error(err)
			return err
		}
	}

	// Create users, grant privileges for current table, setup RLS-policies (if multi-tenant)
	users := getDbUsers(tableName)
	for _, dbUserSpecs := range users {
		if err = db.createDbUser(dbUserSpecs, tableName, record); err != nil {
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
	functionName, functionBody := getCheckAndUpdateRevisionFunc()
	stmt := getCreateTriggerFunctionStmt(functionName, functionBody)
	if tx := db.gormDBMap[dbrole.MAIN].Exec(stmt); tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
		db.logger.Error(err)
		return err
	}

	stmt = getDropTriggerStmt(tableName, functionName)
	if tx := db.gormDBMap[dbrole.MAIN].Exec(stmt); tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
		db.logger.Error(err)
		return err
	}

	stmt = getCreateTriggerStmt(tableName, functionName)
	if tx := db.gormDBMap[dbrole.MAIN].Exec(stmt); tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
		db.logger.Error(err)
		return err
	}

	return nil
}

// Creates a Postgres user (which is the same as role in Postgres). Grants certain privileges to perform certain operations
// to the user (e.g., SELECT only; SELECT, INSERT, UPDATE, DELETE). Creates RLS-policy if the table is multi-tenant.
func (db *relationalDb) createDbUser(dbUserSpecs dbUserSpecs, tableName string, record Record) (err error) {
	stmt := getGrantPrivilegesStmt(tableName, string(dbUserSpecs.username), dbUserSpecs.commands)
	if tx := db.gormDBMap[dbrole.MAIN].Exec(stmt); tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
		db.logger.Error(err)
		return err
	}

	if IsMultitenant(record, tableName) {
		stmt = getCreatePolicyStmt(tableName, record, dbUserSpecs)
		if tx := db.gormDBMap[dbrole.MAIN].Exec(stmt); tx.Error != nil {
			err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
			db.logger.Error(err)
			return err
		}
	}

	return nil
}

func (db *relationalDb) GetAuthorizer() authorizer.Authorizer {
	return db.authorizer
}

// Uses an authorizer to get user's org. ID and a matching DB role.
// With the default MetadataBasedAuthorizer, does the following:
// Gets user's org ID and a DB role that matches one of its CSP roles.
// Returns an error if there are no role mappings for the given table, if user's org. ID cannot be retrieved from CSP,
// or if there is no matching DB role for any one of the user's CSP roles.
func (db *relationalDb) getTenantInfoFromCtx(ctx context.Context, tableNames ...string) (err error, orgId string, dbRole dbrole.DbRole, isDbRoleTenantScoped bool) {
	// Get the matching DB role
	dbRole, err = db.authorizer.GetMatchingDbRole(ctx, tableNames...)
	if err != nil {
		return err, "", "", false
	}

	isDbRoleTenantScoped = dbRole.IsDbRoleTenantScoped()
	orgId, err = db.authorizer.GetOrgFromContext(ctx)
	if !isDbRoleTenantScoped && errors.Is(err, ErrMissingOrgId) {
		err = nil
	}
	if err != nil {
		return err, "", "", false
	}

	db.logger.Infof("Tenant Context: orgId=%s, dbRole=%s, isDbRoleTenantScoped=%v",
		orgId, dbRole, isDbRoleTenantScoped)
	return err, orgId, dbRole, isDbRoleTenantScoped
}

func (db *relationalDb) GetDBConn(dbRole dbrole.DbRole) (*gorm.DB, error) {
	if _, ok := db.gormDBMap[dbRole]; !ok {
		if err := db.initializer(db, dbRole); err != nil {
			err = ErrConnectingToDb.Wrap(err)
			return nil, err
		}
	}
	if conn, ok := db.gormDBMap[dbRole]; ok {
		return conn, nil
	}
	return nil, ErrConnectingToDb.WithValue(DB_ROLE, string(dbRole))
}
