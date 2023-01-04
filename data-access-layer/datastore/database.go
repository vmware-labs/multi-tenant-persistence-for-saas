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
	"hash/fnv"
	"os"
	"reflect"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

// Database roles/users
type DbRole string

type DbRoleSlice []DbRole // Needed for sorting records
func (a DbRoleSlice) Len() int {
	return len(a)
}

func (dbRole DbRole) isTenantDbRole() bool {
	return dbRole == TENANT_READER || dbRole == TENANT_WRITER
}

/*
Returns true if the first role has fewer permissions than the second role, and true if the two roles are the same or
the second role has more permissions.
A reader role is always considered to have fewer permissions than a writer role.
and a tenant-specific reader/writer role is always considered to have fewer permissions than a non-tenant specific reader/writer role, respectively.

TENANT_READER < READER < TENANT_WRITER < WRITER
*/
func (a DbRoleSlice) Less(i, j int) bool {
	var roleI, roleJ string = string(a[i]), string(a[j])
	if roleI == roleJ {
		return true
	} else if strings.Contains(roleI, "reader") && strings.Contains(roleJ, "writer") {
		return true
	} else if strings.Contains(roleI, "writer") && strings.Contains(roleJ, "reader") {
		return false
	} else if strings.Contains(roleI, "reader") && strings.Contains(roleJ, "reader") {
		return DbRole(roleI) == TENANT_READER
	} else if strings.Contains(roleI, "writer") && strings.Contains(roleJ, "writer") {
		return DbRole(roleI) == TENANT_WRITER
	}

	panic("Unable to compare " + roleI + " and " + roleJ)
}
func (a DbRoleSlice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

const (
	// DB Runtime Parameters
	DB_CONFIG_ORG_ID = "multitenant.orgId" // Name of Postgres run-time config. parameter that will store current user's org. ID

	// DB Roles
	TENANT_READER DbRole = "tenant_reader"
	TENANT_WRITER DbRole = "tenant_writer"
	READER        DbRole = "reader"
	WRITER        DbRole = "writer"

	// Env. variable names
	DB_NAME_ENV_VAR           = "DB_NAME"
	DB_PORT_ENV_VAR           = "DB_PORT"
	DB_HOST_ENV_VAR           = "DB_HOST"
	SSL_MODE_ENV_VAR          = "SSL_MODE"
	DB_ADMIN_USERNAME_ENV_VAR = "DB_ADMIN_USERNAME"
	DB_ADMIN_PASSWORD_ENV_VAR = "DB_ADMIN_PASSWORD"
	LOGGER_LEVEL_ENV_VAR      = "LOG_LEVEL"
)

// Specifications for database user
type dbUserSpecs struct {
	username         DbRole // Username/role name (in Postgres, users and roles are equivalent)
	password         string
	policyName       string
	commands         []string // Commands to be permitted in the policy. Could be SELECT, INSERT, UPDATE, DELETE
	existingRowsCond string   // SQL conditional expression to be checked for existing rows. Only those rows for which the condition is true will be visible.
	newRowsCond      string   // SQL conditional expression to be checked for rows being inserted or updated. Only those rows for which the condition is true will be inserted/updated
}

/*
Postgres-backed implementation of DataStore interface. By default, uses MetadataBasedAuthorizer for authentication & authorization.
*/
type RelationalDb struct {
	dbMap      map[DbRole]*sql.DB
	authorizer Authorizer // Allows or cancels operations on the DB depending on user's org. and service roles
}

var relationalDb RelationalDb = RelationalDb{
	dbMap:      make(map[DbRole]*sql.DB, 5 /* init. capacity */),
	authorizer: MetadataBasedAuthorizer{},
}

var logger *logrus.Entry

func init() {
	rawLogger := logrus.New()
	rawLogger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05.000",
	})
	loggerLevel := strings.ToLower(os.Getenv(LOGGER_LEVEL_ENV_VAR))
	if level, err := logrus.ParseLevel(loggerLevel); err != nil {
		rawLogger.SetLevel(logrus.InfoLevel) // Default logging level
	} else {
		rawLogger.SetLevel(level)
	}
	logger = rawLogger.WithField("comp", "saas-persistence")
}

func (database *RelationalDb) initialize() error {

	// Ensure all the needed environment variables are present and non-empty
	for _, envVar := range []string{DB_ADMIN_USERNAME_ENV_VAR, DB_ADMIN_PASSWORD_ENV_VAR, DB_NAME_ENV_VAR, DB_PORT_ENV_VAR, DB_HOST_ENV_VAR, SSL_MODE_ENV_VAR} {
		if _, isPresent := os.LookupEnv(envVar); !isPresent {
			logger.Errorf("Please, provide environment variable %q", envVar)
			return ErrorMissingEnvVar.WithValue(ENV_VAR, envVar)
		}

		if envVarValue := strings.TrimSpace(os.Getenv(envVar)); len(envVarValue) == 0 {
			logger.Errorf("Please, provide a non-empty value for environment variable %q", envVar)
			return ErrorMissingEnvVar.WithValue(ENV_VAR, envVar)
		}
	}

	var err error
	var dbPort int
	var dbAdminUsername = getAdminUsername()
	var dbAdminPassword = strings.TrimSpace(os.Getenv(DB_ADMIN_PASSWORD_ENV_VAR))
	var dbName = strings.TrimSpace(os.Getenv(DB_NAME_ENV_VAR))
	var dbHost = strings.TrimSpace(os.Getenv(DB_HOST_ENV_VAR))
	var sslMode = strings.TrimSpace(os.Getenv(SSL_MODE_ENV_VAR))

	// Ensure port number is valid
	if dbPort, err = strconv.Atoi(strings.TrimSpace(os.Getenv(DB_PORT_ENV_VAR))); err != nil {
		err = InvalidPortNumberError.Wrap(err).WithValue(PORT_NUMBER, os.Getenv(DB_PORT_ENV_VAR))
		logger.Errorf("Please, provide a valid port number in environment variable %q", DB_PORT_ENV_VAR)
		return err
	}

	if _, ok := database.dbMap[dbAdminUsername]; ok {
		return nil
	}

	// Create DB connections
	database.dbMap[dbAdminUsername], err = openDb(dbHost, dbPort, dbAdminUsername, dbAdminPassword, dbName, sslMode)
	if err != nil {
		args := map[ErrorContextKey]string{
			DB_HOST:           dbHost,
			DB_PORT:           strconv.Itoa(dbPort),
			DB_ADMIN_USERNAME: string(dbAdminUsername),
			DB_NAME:           dbName,
			SSL_MODE:          sslMode,
		}
		err = ErrorConnectingToDb.WithMap(args).Wrap(err)
		logger.Error(err)
		return err
	}

	users := getDbUsers("ANY")
	for _, dbUserSpecs := range users {
		stmt := getCreateUserStmt(dbUserSpecs.username, dbUserSpecs.password)
		if _, err = database.dbMap[dbAdminUsername].Exec(stmt); err != nil {
			return err
		}

		database.dbMap[dbUserSpecs.username], err = openDb(dbHost, dbPort, dbUserSpecs.username, dbUserSpecs.password, dbName, sslMode)
		if err != nil {
			args := map[ErrorContextKey]string{
				DB_HOST:     dbHost,
				DB_PORT:     strconv.Itoa(dbPort),
				DB_USERNAME: string(dbUserSpecs.username),
				DB_NAME:     dbName,
				SSL_MODE:    sslMode,
			}
			err = ErrorConnectingToDb.WithMap(args).Wrap(err)
			logger.Error(err)
			return err
		}

	}

	return nil
}

/*
Opens a Postgres DB using the provided config. parameters
*/
func openDb(dbHost string, dbPort int, dbUsername DbRole, dbPassword string, dbName string, sslMode string) (*sql.DB, error) {
	logger.Infof("Connecting to database %s:%d[%s]", dbHost, dbPort, dbName)
	// Create DB connection
	dataSourceName := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", dbHost, dbPort, dbUsername, dbPassword, dbName, sslMode)
	db, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}

	// Ensure DB connection works
	if err = db.Ping(); err != nil {
		return nil, err
	}

	logger.Infof("Successfully established a connection with Postgres DB at %s:%d as user %q", dbHost, dbPort, dbUsername)
	return db, nil
}

/*
Starts a transaction. Sets a runtime config. parameter for org. ID within the scope of the transaction.
*/
func getTx(db *sql.DB, orgId string, isMultitenant bool) (*sql.Tx, error) {
	// Ensure that DB has been previously opened during registration with DAL
	if db == nil {
		logger.Error(ErrorStructNotRegisteredWithDAL.Error())
		return nil, ErrorStructNotRegisteredWithDAL
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	if isMultitenant {
		// Set org. ID
		stmt := getSetConfigStmt(DB_CONFIG_ORG_ID, orgId, true)
		if _, err = tx.Exec(stmt); err != nil {
			return nil, err
		}
	}

	return tx, nil
}

/*
Validates tenancy information and returns a transaction with right dbRole
*/
func (database *RelationalDb) validateAndGetTx(ctx context.Context, tableName string, record Record, sqlStmt string) (*sql.Tx, error) {
	err, orgId, dbRole, isMultitenant := database.getTenantInfoFromCtx(ctx, tableName)
	if err != nil {
		return nil, err
	}

	if err = database.authorizer.IsOperationAllowed(ctx, tableName, record); err != nil {
		logger.Errorf("Operation not allowed for the following reason: %s", err.Error())
		return nil, err
	}

	tx, err := getTx(database.dbMap[dbRole], orgId, isMultitenant)
	if err != nil {
		err = ErrorStartingTx.Wrap(err).WithMap(map[ErrorContextKey]string{
			"db_role":    string(dbRole),
			"table_name": tableName,
			"authorizer": GetTableName(database.authorizer), // This will just get struct's name using reflection
		})
		logger.Error(err)
		return nil, err
	}

	return tx, nil
}

// Resets DB connection pools
func (database *RelationalDb) Reset() {
	for _, db := range database.dbMap {
		if db != nil {
			if err := db.Close(); err != nil {
				panic(err)
			}
		}
	}
	database.dbMap = make(map[DbRole]*sql.DB)
	database.authorizer = MetadataBasedAuthorizer{}
	if err := database.initialize(); err != nil {
		panic(err)
	}
}

/*
Performs an inner join between 2 non-multitenant DB tables, joining them on the key column of the first table and the "record2JoinOnField" column of the 2nd table.
Assumes the following:
- There is a one-to-one relationship between the two tables
- Both tables either have primary keys consisting of one column (ID) or a composite key that is a combination of record ID and org. ID
Requires a unique ID of the record in the first table to be passed, which leads to only one record to be returned.
record1 and record2 must be pointers to structs implementing Record interface and represent entities persisted to the 2 DB tables.
record1 and record2 will be filled with data retrieved from the 2 DB tables.
TODO return ErrOperationNotAllowed if the library user is trying to access other tenant's data
*/
func (database *RelationalDb) PerformJoinOneToOne(ctx context.Context, record1 Record, record1Id string, record2 Record, record2JoinOnColumn string) error {
	if err := validateIdAndRecordPtr(record1Id, record1); err != nil {
		return err
	}

	if err := validateFieldAndRecordPtr(record2JoinOnColumn, "record2JoinOnColumn", record2); err != nil {
		return err
	}

	// Get org. ID and DB role
	table1Name := GetTableName(record1)
	table2Name := GetTableName(record2)

	err, orgId, dbRole, isMultitenant := database.getTenantInfoFromCtx(ctx, table1Name, table2Name)
	if err != nil {
		return err
	}

	query := getSelectJoinStmt(table1Name, table2Name, record1, record2, record2JoinOnColumn, 1)
	tx, err := getTx(database.dbMap[dbRole], orgId, isMultitenant)
	if err != nil {
		err = ErrorStartingTx.Wrap(err)
		logger.Error(err)
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Execute query
	row, err := tx.Query(query, record1Id)
	queryWithArgs := replacePlaceholdersInSqlStmt(query, record1Id) // Replace placeholders with actual values for logging purposes
	if err != nil {
		err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, queryWithArgs)
		logger.Errorln(err)
		return err
	}
	defer row.Close()

	if !row.Next() {
		if err = row.Err(); err != nil {
			err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, queryWithArgs)
			logger.Errorln(err)
			return err
		}
		return nil
	}

	err = parseSqlRows(row, record1, record2)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		err = ErrorCommittingTx.Wrap(err)
		logger.Error(err)
		return err
	}
	return nil
}

/*
Performs an inner join between 2 non-multitenant DB tables, joining them on the key column of the first table and the "record2JoinOnField" column of the 2nd table.
Assumes the following:
- There is a one-to-many relationship between the two tables
- Both tables either have primary keys consisting of one column (ID) or a composite key that is a combination of record ID and org. ID
Requires a unique ID of the record in the first table to be passed, which leads to 1 record from the first table and multiple records
from the second table to be returned.
record1 must be a pointer to a struct implementing Record interface.
query2Output must be a pointer to an array of structs implementing Record interface.
record1 and query2Output will be filled with data retrieved from the 2 DB tables.
*/
func (database *RelationalDb) PerformJoinOneToMany(ctx context.Context, record1 Record, record1Id string, record2JoinOnColumn string, query2Output interface{}) error {
	if err := validateIdAndRecordPtr(record1Id, record1); err != nil {
		return err
	}

	if err := validateFieldAndRecordSlicePtr(record2JoinOnColumn, "record2JoinOnColumn", query2Output, 1); err != nil {
		return err
	}

	record2Type := reflect.ValueOf(query2Output).Elem().Index(0).Type()
	record2 := reflect.New(record2Type).Interface().(Record)

	var err error
	struct2Type := reflect.ValueOf(record2).Elem().Type()

	// Get org. ID and DB role
	table1Name := GetTableName(record1)
	table2Name := GetTableName(record2)

	err, orgId, dbRole, isMultitenant := database.getTenantInfoFromCtx(ctx, table1Name, table2Name)
	if err != nil {
		return err
	}

	// Execute query
	query := getSelectJoinStmt(table1Name, table2Name, record1, record2, record2JoinOnColumn, 0)
	tx, err := getTx(database.dbMap[dbRole], orgId, isMultitenant)
	if err != nil {
		err = ErrorStartingTx.Wrap(err)
		logger.Error(err)
		return err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.Query(query, record1Id)
	queryWithArgs := replacePlaceholdersInSqlStmt(query, record1Id) // Replace placeholders with actual values for logging purposes
	if err != nil {
		err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, queryWithArgs)
		logger.Errorln(err)
		return err
	}
	defer rows.Close()

	// Parse each record
	output := reflect.MakeSlice(reflect.SliceOf(struct2Type), 0, 5)
	for rows.Next() {
		record2 = reflect.New(struct2Type).Interface().(Record)
		err = parseSqlRows(rows, record1, record2)
		if err != nil && err != sql.ErrNoRows {
			return err
		}

		output = reflect.Append(output, reflect.ValueOf(record2).Elem())
	}

	reflect.ValueOf(query2Output).Elem().Set(output)
	if err = tx.Commit(); err != nil {
		err = ErrorCommittingTx.Wrap(err)
		logger.Error(err)
		return err
	}
	return nil
}

/*
Finds a single record that has the same primary key as record.
record argument must be a pointer to a struct and will be modified in-place.
*/
func (database *RelationalDb) Find(ctx context.Context, record Record) error {
	return database.FindInTable(ctx, GetTableName(record), record)
}

/*
Finds a single record in table tableName that has the same primary key as record.
record argument must be a pointer to a struct and will be modified in-place.
*/
func (database *RelationalDb) FindInTable(ctx context.Context, tableName string, record Record) error {
	var err error

	if reflect.TypeOf(record).Kind() != reflect.Ptr {
		errMsg := "\"record\" argument has to be a pointer to a struct implementing \"Record\" interface"
		logger.Error(errMsg)
		err = IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}

	// Execute query
	query := getSelectSingleStmt(tableName, record)
	tx, err := database.validateAndGetTx(ctx, tableName, record, query)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	row, err := tx.Query(query, record.GetId()...)
	queryWithArgs := replacePlaceholdersInSqlStmt(query, record.GetId()...) // Replace placeholders with actual values for logging purposes
	logger.Debugln(queryWithArgs)
	if err != nil {
		err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, queryWithArgs)
		logger.Errorln(err)
		return err
	}
	defer row.Close()

	if !row.Next() {
		if err = row.Err(); err != nil {
			// rows.Next() failed because there is an error, not because query returned no results
			err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, queryWithArgs)
			logger.Errorln(err)
			return err
		}
		return nil
	}
	err = parseSqlRows(row, record)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		err = ErrorCommittingTx.Wrap(err)
		logger.Error(err)
		return err
	}
	return nil
}

/*
Finds all records in a DB table.
records must be a pointer to a slice of structs and will be modified in-place.
*/
func (database *RelationalDb) FindAll(ctx context.Context, records interface{}) error {
	if reflect.TypeOf(records).Kind() != reflect.Ptr || reflect.TypeOf(records).Elem().Kind() != reflect.Slice {
		errMsg := "\"records\" argument has to be a pointer to a slice of structs implementing \"Record\" interface"
		logger.Error(errMsg)
		err := IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}

	tableName := GetTableNameFromSlice(records)
	return database.FindAllInTable(ctx, tableName, records)
}

/*
Finds all records in DB table tableName.
records must be a pointer to a slice of structs and will be modified in-place.
*/
func (database *RelationalDb) FindAllInTable(ctx context.Context, tableName string, records interface{}) error {
	record := GetRecordInstanceFromSlice(records)
	structType := reflect.TypeOf(record)
	if structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
	}
	return database.FindWithFilterInTable(ctx, tableName, reflect.Zero(structType).Interface().(Record), records)
}

/*
Finds multiple records in a DB table.
If record argument is non-empty, uses the non-empty fields as criteria in a query.
records must be a pointer to a slice of structs and will be modified in-place.
*/
func (database *RelationalDb) FindWithFilter(ctx context.Context, record Record, records interface{}) error {
	return database.FindWithFilterInTable(ctx, GetTableName(record), record, records)
}

/*
Finds multiple records in DB table tableName.
If record argument is non-empty, uses the non-empty fields as criteria in a query.
records must be a pointer to a slice of structs and will be modified in-place.
*/
func (database *RelationalDb) FindWithFilterInTable(ctx context.Context, tableName string, record Record, records interface{}) error {
	var err error

	if reflect.TypeOf(records).Kind() != reflect.Ptr || reflect.TypeOf(records).Elem().Kind() != reflect.Slice {
		errMsg := "\"records\" argument has to be a pointer to a slice of structs implementing \"Record\" interface"
		logger.Error(errMsg)
		err = IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}

	structValue := reflect.ValueOf(record)
	if structValue.Kind() == reflect.Ptr {
		structValue = structValue.Elem()
	}

	// Execute query
	query, args := getSelectStmt(tableName, record)
	tx, err := database.validateAndGetTx(ctx, tableName, record, query)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.Query(query, args...)
	queryWithArgs := replacePlaceholdersInSqlStmt(query, args...) // Replace placeholders with actual values for logging purposes
	if err != nil {
		err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, queryWithArgs)
		logger.Errorln(err)
		return err
	}
	defer rows.Close()

	// Parse each record
	output := reflect.MakeSlice(reflect.SliceOf(structValue.Type()), 0, 5 /* initial capacity */)
	for rows.Next() {
		recordCopy := reflect.New(structValue.Type()).Interface().(Record)
		err = parseSqlRows(rows, recordCopy)
		if err == nil {
			output = reflect.Append(output, reflect.ValueOf(recordCopy).Elem())
		} else if err != nil && err != sql.ErrNoRows {
			return err
		}
	}

	reflect.ValueOf(records).Elem().Set(output)
	if err = tx.Commit(); err != nil {
		err = ErrorCommittingTx.Wrap(err)
		logger.Error(err)
		return err
	}
	return nil
}

/*
Inserts a record into a DB table
*/
func (database *RelationalDb) Insert(ctx context.Context, record Record) (int64, error) {
	return database.InsertInTable(ctx, GetTableName(record), record)
}

/*
Inserts a record into DB table tableName.
*/
func (database *RelationalDb) InsertInTable(ctx context.Context, tableName string, record Record) (int64, error) {
	// Execute query
	stmt, args := getInsertStmt(tableName, record)
	tx, err := database.validateAndGetTx(ctx, tableName, record, stmt)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.Exec(stmt, args...)
	stmtWithArgs := replacePlaceholdersInSqlStmt(stmt, args...) // Replace placeholders with actual values for logging purposes
	if err != nil {
		err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmtWithArgs)
		logger.Errorln(err)
		return 0, err
	}

	_, err = result.RowsAffected()
	if err != nil {
		err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmtWithArgs)
		logger.Errorln(err)
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		err = ErrorCommittingTx.Wrap(err)
		logger.Error(err)
		return 0, err
	}
	var rowsAffected, _ = result.RowsAffected()
	return rowsAffected, nil
}

/*
Deletes a record from a DB table
*/
func (database *RelationalDb) Delete(ctx context.Context, record Record) (int64, error) {
	return database.DeleteInTable(ctx, GetTableName(record), record)
}

/*
 * Drops the DB tables given by Records
 */
func (database *RelationalDb) DropTables(records ...Record) error {
	// Initialize DB connection
	if err := relationalDb.initialize(); err != nil {
		return err
	}

	if _, ok := os.LookupEnv(DB_ADMIN_USERNAME_ENV_VAR); !ok {
		return ErrorMissingEnvVar.WithValue(ENV_VAR, DB_ADMIN_USERNAME_ENV_VAR)
	}

	adminUsername := getAdminUsername()

	// Drop DB tables
	for _, record := range records {
		tableName := GetTableName(record)
		stmt := getDropTableStmt(tableName)
		if _, err := relationalDb.dbMap[adminUsername].Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

/*
Drops given DB tables.
*/
func (database *RelationalDb) Drop(tableNames ...string) error {
	// Initialize DB connection
	if err := relationalDb.initialize(); err != nil {
		return err
	}

	if _, ok := os.LookupEnv(DB_ADMIN_USERNAME_ENV_VAR); !ok {
		return ErrorMissingEnvVar.WithValue(ENV_VAR, DB_ADMIN_USERNAME_ENV_VAR)
	}

	adminUsername := getAdminUsername()

	// Drop DB tables
	for _, tableName := range tableNames {
		stmt := getDropTableStmt(tableName)
		if _, err := relationalDb.dbMap[adminUsername].Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (database *RelationalDb) Truncate(tableNames ...string) error {
	// Initialize DB connection
	if err := relationalDb.initialize(); err != nil {
		return err
	}

	if _, ok := os.LookupEnv(DB_ADMIN_USERNAME_ENV_VAR); !ok {
		return ErrorMissingEnvVar.WithValue(ENV_VAR, DB_ADMIN_USERNAME_ENV_VAR)
	}

	adminUsername := getAdminUsername()

	// Truncate DB tables
	for _, tableName := range tableNames {
		stmt := getTruncateTableStmt(tableName)
		if _, err := relationalDb.dbMap[adminUsername].Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

/*
Deletes a record from DB table tableName
*/
func (database *RelationalDb) DeleteInTable(ctx context.Context, tableName string, record Record) (int64, error) {
	stmt := getDeleteStmt(tableName, record)
	tx, err := database.validateAndGetTx(ctx, tableName, record, stmt)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	// Execute query
	result, err := tx.Exec(stmt, record.GetId()...)
	stmtWithArgs := replacePlaceholdersInSqlStmt(stmt, record.GetId()...) // Replace placeholders with actual values for logging purposes
	if err != nil {
		err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmtWithArgs)
		logger.Errorln(err)
		return 0, err
	}
	_, err = result.RowsAffected()
	if err != nil {
		err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmtWithArgs)
		logger.Errorln(err)
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		err = ErrorCommittingTx.Wrap(err)
		logger.Error(err)
		return 0, err
	}

	var rowsAffected, _ = result.RowsAffected()
	return rowsAffected, nil
}

/*
Updates a record in a DB table
*/
func (database *RelationalDb) Update(ctx context.Context, record Record) (int64, error) {
	return database.UpdateInTable(ctx, GetTableName(record), record)
}

/*
Updates a record in DB table tableName
*/
func (database *RelationalDb) UpdateInTable(ctx context.Context, tableName string, record Record) (int64, error) {
	// Execute query
	stmt, args := getUpdateStmt(tableName, record)
	tx, err := database.validateAndGetTx(ctx, tableName, record, stmt)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.Exec(stmt, args...)
	stmtWithArgs := replacePlaceholdersInSqlStmt(stmt, args...) // Replace placeholders with actual values for logging purposes
	if err != nil {
		if strings.Contains(err.Error(), REVISION_OUTDATED_MSG) {
			err = RevisionConflictError.Wrap(err).WithValue(SQL_STMT, stmtWithArgs)
		} else {
			err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmtWithArgs)
		}
		logger.Errorln(err)
		return 0, err
	}
	_, err = result.RowsAffected()
	if err != nil {
		err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmtWithArgs)
		logger.Errorln(err)
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		err = ErrorCommittingTx.Wrap(err)
		logger.Error(err)
		return 0, err
	}
	var rowsAffected, _ = result.RowsAffected()
	return rowsAffected, nil
}

/*
Upserts a record in a DB table
*/
func (database *RelationalDb) Upsert(ctx context.Context, record Record) (int64, error) {
	return database.UpsertInTable(ctx, GetTableName(record), record)
}

/*
Upserts a record in DB table tableName
*/
func (database *RelationalDb) UpsertInTable(ctx context.Context, tableName string, record Record) (int64, error) {
	// Execute query
	stmt, args := getUpsertStmt(tableName, record)
	tx, err := database.validateAndGetTx(ctx, tableName, record, stmt)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.Exec(stmt, args...)
	stmtWithArgs := replacePlaceholdersInSqlStmt(stmt, args...) // Replace placeholders with actual values for logging purposes
	if err != nil {
		if strings.Contains(err.Error(), REVISION_OUTDATED_MSG) {
			err = RevisionConflictError.Wrap(err).WithValue(SQL_STMT, stmtWithArgs)
		} else {
			err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmtWithArgs)
		}
		logger.Errorln(err)
		return 0, err
	}
	_, err = result.RowsAffected()
	if err != nil {
		err = ErrorExecutingSqlStmt.Wrap(err).WithValue(SQL_STMT, stmtWithArgs)
		logger.Errorln(err)
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		err = ErrorCommittingTx.Wrap(err)
		logger.Error(err)
		return 0, err
	}
	var rowsAffected, _ = result.RowsAffected()
	return rowsAffected, nil
}

// Registers a struct with DAL. See RegisterWithDALHelper() for more info.
func (database *RelationalDb) RegisterWithDAL(ctx context.Context, roleMapping map[string]DbRole, record Record) error {
	return database.RegisterWithDALHelper(ctx, roleMapping, GetTableName(record), record)
}

/*
Create a DB table for the given struct. Enables RLS in it if it is multi-tenant.
Generates Postgres roles and policies based on the provided role mapping and applies them
to the created DB table.
roleMapping - maps service roles to DB roles to be used for the generated DB table
There are 4 possible DB roles to choose from:
- READER, which gives read access to all the records in the table
- WRITER*, which gives read & write access to all the records in the table
- TENANT_READER, which gives read access to current tenant's records
- TENANT_WRITER, which gives read & write access to current tenant's records

Panics if the struct doesn't pass the following validations:
- Each field has to have a tag indicating the name of the relevant column in DB table
- Each field has to be exported
- Each field has to be of a supported data type (signed integral data types, boolean, strings)
*/
func (database *RelationalDb) RegisterWithDALHelper(_ context.Context, roleMapping map[string]DbRole, tableName string, record Record) error {
	if roleMapping == nil {
		roleMapping = make(map[string]DbRole)
	}

	logger.Debugf("Registering the struct %q with DAL (backed by Postgres)... Using authorizer %s...", tableName, GetTableName(database.authorizer))

	// Fill up caches in sql_struct.go
	_ = getDatabaseColumns(tableName, record)
	_ = getPrimaryKey(tableName, record)

	adminUsername := getAdminUsername()

	// Connect to Postgres if there are no connections
	if err := database.initialize(); err != nil {
		err = ErrorRegisteringWithDAL.Wrap(err).WithValue(TABLE_NAME, tableName)
		logger.Error(err)
		return err
	}

	var err error
	stmt := getCreateTableStmt(tableName, record)
	if _, err = database.dbMap[adminUsername].Exec(stmt); err != nil {
		err = ErrorRegisteringWithDAL.Wrap(err).WithValue(TABLE_NAME, tableName)
		logger.Error(err)
		return err
	}

	// Set up trigger on _revision column for tables that need it
	if isRevisioningSupported(tableName, record) {
		if err = database.enforceRevisioning(tableName); err != nil {
			return err
		}
	}

	// Enable row-level security in a multi-tenant table
	if isMultitenant(record, tableName) {
		stmt = getEnableRLSStmt(tableName, record)
		if _, err = database.dbMap[adminUsername].Exec(stmt); err != nil {
			err = ErrorRegisteringWithDAL.Wrap(err).WithValue(TABLE_NAME, tableName)
			logger.Error(err)
			return err
		}
	}

	// Create users, grant privileges for current table, setup RLS-policies (if multi-tenant)
	users := getDbUsers(tableName)
	for _, dbUserSpecs := range users {
		if err = database.createDbUser(dbUserSpecs, tableName, record); err != nil {
			err = ErrorRegisteringWithDAL.Wrap(err).WithValue(TABLE_NAME, tableName)
			return err
		}
	}

	database.authorizer.Configure(tableName, roleMapping)
	return nil
}

/*
Creates a Postgres trigger that checks if a record being updated contains the most recent revision.
If not, update is rejected.
*/
func (database *RelationalDb) enforceRevisioning(tableName string) error {
	adminUsername := getAdminUsername()

	stmt := getCreateRevisionFunctionStmt()
	if _, err := database.dbMap[adminUsername].Exec(stmt); err != nil {
		return err
	}
	stmt = getDropIfExistsRevisionTriggerStmt(tableName)
	if _, err := database.dbMap[adminUsername].Exec(stmt); err != nil {
		return err
	}
	stmt = getCreateRevisionTriggerStmt(tableName)
	if _, err := database.dbMap[adminUsername].Exec(stmt); err != nil {
		return err
	}

	return nil
}

/*
Creates a Postgres user (which is the same as role in Postgres). Grants certain privileges to perform certain operations
to the user (e.g., SELECT only; SELECT, INSERT, UPDATE, DELETE). Creates RLS-policy if the table is multi-tenant.
*/
func (database *RelationalDb) createDbUser(dbUserSpecs dbUserSpecs, tableName string, record Record) error {
	adminUsername := getAdminUsername()

	stmt := getGrantPrivilegesStmt(tableName, record, dbUserSpecs.username, dbUserSpecs.commands)
	if _, err := database.dbMap[adminUsername].Exec(stmt); err != nil {
		// err = ErrorRegisteringWithDAL.Wrap(err).WithValue(TABLE_NAME, tableName)
		logger.Error(err)
		return err
	}

	if isMultitenant(record, tableName) {
		stmt = getCreatePolicyStmt(tableName, record, dbUserSpecs)
		if _, err := database.dbMap[adminUsername].Exec(stmt); err != nil {
			// err = ErrorRegisteringWithDAL.Wrap(err).WithValue(TABLE_NAME, tableName)
			logger.Error(err)
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
- user with read & write access to all orgs
*/
func getDbUsers(tableName string) []dbUserSpecs {
	writer := dbUserSpecs{
		username:         WRITER,
		commands:         []string{"SELECT", "INSERT", "UPDATE", "DELETE"},
		existingRowsCond: "true", // Allow access to all existing rows
		newRowsCond:      "true", // Allow inserting or updating records
	}

	reader := dbUserSpecs{
		username:         READER,
		commands:         []string{"SELECT"}, // READER role will only be able to perform SELECT
		existingRowsCond: "true",             // Allow access to all existing rows
		newRowsCond:      "false",            // Prevent inserting or updating records
	}

	tenantWriter := dbUserSpecs{
		username:         TENANT_WRITER,
		commands:         []string{"SELECT", "INSERT", "UPDATE", "DELETE"},
		existingRowsCond: ORG_ID_COLUMN_NAME + " = current_setting('" + DB_CONFIG_ORG_ID + "')", // Allow access only to its tenant's records
		newRowsCond:      ORG_ID_COLUMN_NAME + " = current_setting('" + DB_CONFIG_ORG_ID + "')", // Allow inserting for or updating records of its own tenant
	}

	tenantReader := dbUserSpecs{
		username:         TENANT_READER,
		commands:         []string{"SELECT"},                                                    // TENANT_READER role will only be able to perform SELECT on its tenant's records
		existingRowsCond: ORG_ID_COLUMN_NAME + " = current_setting('" + DB_CONFIG_ORG_ID + "')", // Allow access only to its tenant's records
		newRowsCond:      "false",                                                               // Prevent inserting or updating records
	}

	dbUsers := []dbUserSpecs{writer, reader, tenantWriter, tenantReader}
	for i := 0; i < len(dbUsers); i++ {
		dbUsers[i].password = getPassword(dbUsers[i].username)
		dbUsers[i].policyName = GetRlsPolicyName(dbUsers[i].username, tableName)
	}
	return dbUsers
}

/*
Generates a password for a DB user by getting a hash of DB admin. password concatenated with a DB username and
converting the hash to hex
*/
func getPassword(username DbRole) string {
	h := fnv.New32a()
	h.Write([]byte(os.Getenv(DB_ADMIN_PASSWORD_ENV_VAR) + string(username)))
	return strconv.FormatInt(int64(h.Sum32()), 16)
}

func (database *RelationalDb) GetAuthorizer() Authorizer {
	return database.authorizer
}

/*
Configures data store to use Postgres or an in-memory cache. Should be called when microservice starts up.
*/
func (database *RelationalDb) Configure(_ context.Context, isDataStoreInMemory bool, authorizer Authorizer) {
	configureDataStore(isDataStoreInMemory, authorizer)
}

/*
Parses a SQL row and fills provided record(s) with the relevant values.
If multiple records are provided, it is assumed that the columns in SQL row are ordered the same way that the fields
in given structs are.
The record must be pointers to structs.
*/
func parseSqlRows(rows *sql.Rows, records ...Record) error {
	var initCapacity = reflect.ValueOf(records[0]).Elem().NumField()
	dest := make([]interface{}, 0, initCapacity)

	// TODO(jeyhun): Ordering of DB vs struct is tightly coupled here, will
	// need fixes when the structs and aggregate queries show up
	for _, record := range records {
		structValue := reflect.ValueOf(record).Elem()
		for i := 0; i < structValue.NumField(); i++ {
			dest = append(dest, getInstanceFromKind(structValue.Field(i).Kind()))
		}
	}

	err := rows.Scan(dest...)

	if err == sql.ErrNoRows {
		return nil
	} else if err != nil {
		err = ErrorParsingSqlRow.Wrap(err)
		logger.Error(err)
		return err
	}

	var destIndex = 0
	for _, record := range records {
		structValue := reflect.ValueOf(record).Elem()
		for i := 0; i < structValue.NumField(); i++ {
			updateValueBasedOnKind(structValue.Field(i), dest[destIndex])
			destIndex++
		}
	}

	return nil
}

/*
Uses an authorizer to get user's org. ID and a matching DB role.
With the default MetadataBasedAuthorizer, does the following:
Gets user's org ID and a DB role that matches one of its CSP roles.
Returns an error if there are no role mappings for the given table, if user's org. ID cannot be retrieved from CSP,
or if there is no matching DB role for any one of the user's CSP roles.
*/
func (database *RelationalDb) getTenantInfoFromCtx(ctx context.Context, tableNames ...string) (err error, orgId string, dbRole DbRole, isMultitenant bool) {
	// Get the matching DB role
	dbRole, err = database.authorizer.GetMatchingDbRole(ctx, tableNames...)
	if err != nil {
		return err, "", "", false
	}

	isMultitenant = dbRole.isTenantDbRole()
	orgId, err = database.authorizer.GetOrgFromContext(ctx)
	if !isMultitenant && errors.Is(err, ErrorMissingOrgId) {
		err = nil
	}
	if err != nil {
		return err, "", "", false
	}

	logger.Infof("TenantContext for DB transaction orgId=%s, dbRole=%s, isMultitenant=%v", orgId, dbRole, isMultitenant)
	return err, orgId, dbRole, isMultitenant
}

func getAdminUsername() DbRole {
	return DbRole(strings.TrimSpace(os.Getenv(DB_ADMIN_USERNAME_ENV_VAR)))
}
