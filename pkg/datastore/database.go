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
	"hash/fnv"
	"os"
	"reflect"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

const (
	// DB Runtime Parameters.
	DB_CONFIG_ORG_ID = "multitenant.orgId" // Name of Postgres run-time config. parameter that will store current user's org. ID

	// Env. variable names.
	DB_NAME_ENV_VAR           = "DB_NAME"
	DB_PORT_ENV_VAR           = "DB_PORT"
	DB_HOST_ENV_VAR           = "DB_HOST"
	SSL_MODE_ENV_VAR          = "SSL_MODE"
	DB_ADMIN_USERNAME_ENV_VAR = "DB_ADMIN_USERNAME"
	DB_ADMIN_PASSWORD_ENV_VAR = "DB_ADMIN_PASSWORD"
)

// Specifications for database user.
type dbUserSpecs struct {
	username         dbrole.DbRole // Username/role name (in Postgres, users and roles are equivalent)
	password         string
	policyName       string
	commands         []string // Commands to be permitted in the policy. Could be SELECT, INSERT, UPDATE, DELETE
	existingRowsCond string   // SQL conditional expression to be checked for existing rows. Only those rows for which the condition is true will be visible.
	newRowsCond      string   // SQL conditional expression to be checked for rows being inserted or updated. Only those rows for which the condition is true will be inserted/updated
}

/*
Postgres-backed implementation of DataStore interface. By default, uses MetadataBasedAuthorizer for authentication & authorization.
*/
type relationalDb struct {
	authorizer authorizer.Authorizer // Allows or cancels operations on the DB depending on user's org. and service roles
	gormDBMap  map[dbrole.DbRole]*gorm.DB
	logger     *logrus.Entry
}

func GetDefaultDataStore(logger *logrus.Entry, authorizer authorizer.Authorizer) (d DataStore, err error) {
	db := &relationalDb{
		authorizer: authorizer,
		gormDBMap:  make(map[dbrole.DbRole]*gorm.DB),
		logger:     logger,
	}
	err = db.Initialize()
	if err != nil {
		logger.Errorf("Failed to initialize db: %e", err)
	}
	d = db
	return
}

func (db *relationalDb) TestHelper() DataStoreTestHelper {
	return db
}

func (db *relationalDb) Helper() DataStoreHelper {
	return db
}

func (db *relationalDb) Initialize() error {
	// Ensure all the needed environment variables are present and non-empty
	for _, envVar := range []string{DB_ADMIN_USERNAME_ENV_VAR, DB_ADMIN_PASSWORD_ENV_VAR, DB_NAME_ENV_VAR, DB_PORT_ENV_VAR, DB_HOST_ENV_VAR, SSL_MODE_ENV_VAR} {
		if _, isPresent := os.LookupEnv(envVar); !isPresent {
			db.logger.Errorf("Please, provide environment variable %q", envVar)
			return ErrMissingEnvVar.WithValue(ENV_VAR, envVar)
		}

		if envVarValue := strings.TrimSpace(os.Getenv(envVar)); len(envVarValue) == 0 {
			db.logger.Errorf("Please, provide a non-empty value for environment variable %q", envVar)
			return ErrMissingEnvVar.WithValue(ENV_VAR, envVar)
		}
	}

	var err error
	var dbPort int
	dbAdminUsername := getAdminUsername()
	dbAdminPassword := strings.TrimSpace(os.Getenv(DB_ADMIN_PASSWORD_ENV_VAR))
	dbName := strings.TrimSpace(os.Getenv(DB_NAME_ENV_VAR))
	dbHost := strings.TrimSpace(os.Getenv(DB_HOST_ENV_VAR))
	sslMode := strings.TrimSpace(os.Getenv(SSL_MODE_ENV_VAR))

	// Ensure port number is valid
	if dbPort, err = strconv.Atoi(strings.TrimSpace(os.Getenv(DB_PORT_ENV_VAR))); err != nil {
		err = ErrInvalidPortNumber.Wrap(err).WithValue(PORT_NUMBER, os.Getenv(DB_PORT_ENV_VAR))
		db.logger.Errorf("Please, provide a valid port number in environment variable %q", DB_PORT_ENV_VAR)
		return err
	}

	if _, ok := db.gormDBMap[dbAdminUsername]; ok {
		return nil
	}

	// Create DB connections
	db.gormDBMap[dbAdminUsername], err = openDb(dbHost, dbPort, dbAdminUsername, dbAdminPassword, dbName, sslMode)
	if err != nil {
		args := map[ErrorContextKey]string{
			DB_HOST:           dbHost,
			DB_PORT:           strconv.Itoa(dbPort),
			DB_ADMIN_USERNAME: string(dbAdminUsername),
			DB_NAME:           dbName,
			SSL_MODE:          sslMode,
		}
		err = ErrConnectingToDb.WithMap(args).Wrap(err)
		db.logger.Error(err)
		return err
	}

	users := getDbUsers("ANY")
	for _, dbUserSpecs := range users {
		stmt := getCreateUserStmt(string(dbUserSpecs.username), dbUserSpecs.password)
		if tx := db.gormDBMap[dbAdminUsername].Exec(stmt); tx.Error != nil {
			err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
			db.logger.Errorln(err)
			return err
		}

		db.logger.Infof("Connecting to database %s@%s:%d[%s] ...", dbUserSpecs.username, dbHost, dbPort, dbName)
		db.gormDBMap[dbUserSpecs.username], err = openDb(dbHost, dbPort, dbUserSpecs.username, dbUserSpecs.password, dbName, sslMode)
		if err != nil {
			args := map[ErrorContextKey]string{
				DB_HOST:     dbHost,
				DB_PORT:     strconv.Itoa(dbPort),
				DB_USERNAME: string(dbUserSpecs.username),
				DB_NAME:     dbName,
				SSL_MODE:    sslMode,
			}
			err = ErrConnectingToDb.WithMap(args).Wrap(err)
			db.logger.Error(err)
			return err
		}
		db.logger.Infof("Connecting to database %s@%s:%d[%s] successfully", dbUserSpecs.username, dbHost, dbPort, dbName)
	}

	return nil
}

// Opens a Postgres DB using the provided config. parameters.
func openDb(dbHost string, dbPort int, dbUsername dbrole.DbRole, dbPassword string, dbName string, sslMode string) (tx *gorm.DB, err error) {
	// Create DB connection
	dataSourceName := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", dbHost, dbPort, dbUsername, dbPassword, dbName, sslMode)
	db, err := gorm.Open(postgres.Open(dataSourceName),
		&gorm.Config{
			Logger: logger.Default.LogMode(logger.Info),
		},
	)
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	// Ensure DB connection works
	if err = sqlDB.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func TypeName(x interface{}) (typeName string) {
	t := reflect.TypeOf(x)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
		typeName += "*"
	}
	typeName += t.Name()
	return
}

func IsPointerToStruct(x interface{}) (isPtrType bool) {
	t := reflect.TypeOf(x)
	isPtrType = (t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct)
	return
}

// Validates tenancy information and returns a transaction with right dbRole.
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
	// If operation is SELECT but no specific tenant's data is being queried (e.g., FindAll() was called), allow the operation to proceed.
	if dbRole.IsDbRoleTenantScoped() && IsMultitenant(record, tableName) {
		orgIdCol, _ := GetOrgId(record)
		err = ErrOperationNotAllowed.WithValue("tenant", orgId).WithValue("orgIdCol", orgIdCol)
		db.logger.Error(err.Error())
		if orgIdCol != "" && orgIdCol != orgId {
			err = ErrOperationNotAllowed.WithValue("tenant", orgId).WithValue("orgIdCol", orgIdCol)
			db.logger.Error(err)
			return nil, err
		}
	}

	tx = db.gormDBMap[dbRole].Begin()
	if isDbRoleTenantScoped {
		// Set org. ID
		stmt := getSetConfigStmt(DB_CONFIG_ORG_ID, orgId)
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

	tx = tx.Table(tableName)
	return tx, nil
}

// Resets DB connection pools.
func (db *relationalDb) Reset() {
	db.gormDBMap = make(map[dbrole.DbRole]*gorm.DB)
	db.authorizer = authorizer.MetadataBasedAuthorizer{}
	if err := db.Initialize(); err != nil {
		panic(err)
	}
}

// Finds a single record that has the same values as non-zero fields in the record.
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

// Finds all records in DB table tableName.
// records must be a pointer to a slice of structs and will be modified in-place.
func (db *relationalDb) FindAllInTable(ctx context.Context, tableName string, records interface{}) error {
	record := GetRecordInstanceFromSlice(records)
	return db.FindWithFilterInTable(ctx, tableName, record, records)
}

// Finds multiple records in a DB table.
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
	tableNames := make([]string, 0, len(records))
	for _, record := range records {
		tableNames = append(tableNames, GetTableName(record))
	}
	return db.DropCascade(false, tableNames...)
}

/*
Drops given DB tables.
*/
func (db *relationalDb) Drop(tableNames ...string) error {
	return db.DropCascade(false, tableNames...)
}

func (db *relationalDb) DropCascade(cascade bool, tableNames ...string) (err error) {
	// Initialize DB connection
	if err = db.Initialize(); err != nil {
		return err
	}

	if _, ok := os.LookupEnv(DB_ADMIN_USERNAME_ENV_VAR); !ok {
		return ErrMissingEnvVar.WithValue(ENV_VAR, DB_ADMIN_USERNAME_ENV_VAR)
	}

	adminUsername := getAdminUsername()

	// Drop DB tables
	for _, tableName := range tableNames {
		err = db.gormDBMap[adminUsername].Migrator().DropTable(tableName)
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
	// Initialize DB connection
	if err = db.Initialize(); err != nil {
		return err
	}

	if _, ok := os.LookupEnv(DB_ADMIN_USERNAME_ENV_VAR); !ok {
		return ErrMissingEnvVar.WithValue(ENV_VAR, DB_ADMIN_USERNAME_ENV_VAR)
	}

	adminUsername := getAdminUsername()

	// Truncate DB tables
	for _, tableName := range tableNames {
		stmt := getTruncateTableStmt(tableName, cascade)
		if tx := db.gormDBMap[adminUsername].Exec(stmt); tx.Error != nil {
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

	adminUsername := getAdminUsername()

	// Connect to Postgres if there are no connections
	if err = db.Initialize(); err != nil {
		err = ErrRegisteringStruct.Wrap(err).WithValue(TABLE_NAME, tableName)
		db.logger.Error(err)
		return err
	}

	err = db.gormDBMap[adminUsername].Table(tableName).AutoMigrate(record)
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
		if tx := db.gormDBMap[adminUsername].Exec(stmt); tx.Error != nil {
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
	adminUsername := getAdminUsername()

	functionName, functionBody := getCheckAndUpdateRevisionFunc()
	stmt := getCreateTriggerFunctionStmt(functionName, functionBody)
	if tx := db.gormDBMap[adminUsername].Exec(stmt); tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
		db.logger.Error(err)
		return err
	}

	stmt = getDropTriggerStmt(tableName, functionName)
	if tx := db.gormDBMap[adminUsername].Exec(stmt); tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
		db.logger.Error(err)
		return err
	}

	stmt = getCreateTriggerStmt(tableName, functionName)
	if tx := db.gormDBMap[adminUsername].Exec(stmt); tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
		db.logger.Error(err)
		return err
	}

	return nil
}

// Creates a Postgres user (which is the same as role in Postgres). Grants certain privileges to perform certain operations
// to the user (e.g., SELECT only; SELECT, INSERT, UPDATE, DELETE). Creates RLS-policy if the table is multi-tenant.
func (db *relationalDb) createDbUser(dbUserSpecs dbUserSpecs, tableName string, record Record) (err error) {
	adminUsername := getAdminUsername()

	stmt := getGrantPrivilegesStmt(tableName, string(dbUserSpecs.username), dbUserSpecs.commands)
	if tx := db.gormDBMap[adminUsername].Exec(stmt); tx.Error != nil {
		err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
		db.logger.Error(err)
		return err
	}

	if IsMultitenant(record, tableName) {
		stmt = getCreatePolicyStmt(tableName, record, dbUserSpecs)
		if tx := db.gormDBMap[adminUsername].Exec(stmt); tx.Error != nil {
			err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
			db.logger.Error(err)
			return err
		}
	}

	return nil
}

/*
Generates specifications of 4 DB users:
- user with read-only access to his org
- user with read & write access to his org
- user with read-only access to all orgs
- user with read & write access to all orgs.
*/
func getDbUsers(tableName string) []dbUserSpecs {
	writer := dbUserSpecs{
		username:         dbrole.WRITER,
		commands:         []string{"SELECT", "INSERT", "UPDATE", "DELETE"},
		existingRowsCond: "true", // Allow access to all existing rows
		newRowsCond:      "true", // Allow inserting or updating records
	}

	reader := dbUserSpecs{
		username:         dbrole.READER,
		commands:         []string{"SELECT"}, // READER role will only be able to perform SELECT
		existingRowsCond: "true",             // Allow access to all existing rows
		newRowsCond:      "false",            // Prevent inserting or updating records
	}

	tenantWriter := dbUserSpecs{
		username:         dbrole.TENANT_WRITER,
		commands:         []string{"SELECT", "INSERT", "UPDATE", "DELETE"},
		existingRowsCond: COLUMN_ORGID + " = current_setting('" + DB_CONFIG_ORG_ID + "')", // Allow access only to its tenant's records
		newRowsCond:      COLUMN_ORGID + " = current_setting('" + DB_CONFIG_ORG_ID + "')", // Allow inserting for or updating records of its own tenant
	}

	tenantReader := dbUserSpecs{
		username:         dbrole.TENANT_READER,
		commands:         []string{"SELECT"},                                              // TENANT_READER role will only be able to perform SELECT on its tenant's records
		existingRowsCond: COLUMN_ORGID + " = current_setting('" + DB_CONFIG_ORG_ID + "')", // Allow access only to its tenant's records
		newRowsCond:      "false",                                                         // Prevent inserting or updating records
	}

	dbUsers := []dbUserSpecs{writer, reader, tenantWriter, tenantReader}
	for i := 0; i < len(dbUsers); i++ {
		dbUsers[i].password = getPassword(string(dbUsers[i].username))
		dbUsers[i].policyName = GetRlsPolicyName(string(dbUsers[i].username), tableName)
	}
	return dbUsers
}

// Generates a password for a DB user by getting a hash of DB admin. password concatenated with a DB username and
// converting the hash to hex.
func getPassword(username string) string {
	h := fnv.New32a()
	h.Write([]byte(os.Getenv(DB_ADMIN_PASSWORD_ENV_VAR) + username))
	return strconv.FormatInt(int64(h.Sum32()), 16)
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

	db.logger.Infof("TenantContext for DB transaction orgId=%s, dbRole=%s, isDbRoleTenantScoped=%v", orgId, dbRole, isDbRoleTenantScoped)
	return err, orgId, dbRole, isDbRoleTenantScoped
}

func getAdminUsername() dbrole.DbRole {
	return dbrole.DbRole(strings.TrimSpace(os.Getenv(DB_ADMIN_USERNAME_ENV_VAR)))
}
