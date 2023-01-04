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

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

/*
Library processing all the golang tags in the struct into SQL domain
*/

const (
	// Golang Tags
	TAG_DB_COLUMN = "db_column"
	TAG_PRIMARY   = "primary_key"
	TAG_INDEX     = "db_index"

	// SQL Columns
	ORG_ID_COLUMN_NAME   = "org_id"
	REVISION_COLUMN_NAME = "_revision"

	// Messages
	REVISION_OUTDATED_MSG = "Invalid update - outdated "
)

type DatabaseColumn struct {
	name     string
	dataType string
	primary  bool
	index    bool
}

// Maps a DB table name to its columns
var databaseColumns map[string]*[]DatabaseColumn = make(map[string]*[]DatabaseColumn)

// Maps a DB table name to booleans showing if the table is multitenant
var multitenancyMap map[string]bool = make(map[string]bool)

// Maps a DB table name to the names of DB columns that comprise the primary key
var primaryKeyMap map[string][]string = make(map[string][]string)

// Maps DB table name to boolean showing if the table is concurrent with _revision column
var revisionSupportMap map[string]bool = make(map[string]bool)

/*
Checks if revisioning is supported in the given table (if
the struct contains a field with tag "db_column" equal to "_revision")
*/
func isRevisioningSupported(tableName string, x Record) bool {
	if _, ok := revisionSupportMap[tableName]; !ok {
		_, revisionSupportMap[tableName] = getFieldNoPanic(REVISION_COLUMN_NAME, x)
	}
	return revisionSupportMap[tableName]
}

/*
Checks if any of the tables in tableNames are multi-tenant.
*/
func isMultitenant(x Record, tableNames ...string) bool {
	for _, tableName := range tableNames {
		if _, ok := multitenancyMap[tableName]; !ok {
			_, multitenancyMap[tableName] = getFieldNoPanic(ORG_ID_COLUMN_NAME, x)
		}
	}

	var isMultitenant bool = false
	for i := 0; i < len(tableNames) && !isMultitenant; i++ {
		isMultitenant = multitenancyMap[tableNames[i]]
	}
	return isMultitenant
}

/*
Returns columns comprising a primary key of a table
*/
func getPrimaryKey(tableName string, x Record) []string {
	if _, ok := primaryKeyMap[tableName]; !ok {
		primaryKeyMap[tableName] = make([]string, 0, 3 /* Init. capacity */)
		for _, dbColumn := range *getDatabaseColumns(tableName, x) {
			if dbColumn.primary {
				primaryKeyMap[tableName] = append(primaryKeyMap[tableName], dbColumn.name)
			}
		}

		if len(primaryKeyMap[tableName]) == 0 {
			logger.Panicf("Primary key not detected for table %s", tableName)
			panic(tableName)
		}
	}

	return primaryKeyMap[tableName]
}

func getBool(str string) bool {
	if len(str) == 0 {
		return false
	}
	b, err := strconv.ParseBool(str)
	if err != nil {
		logger.Warnf("Failed to parse the string %v to boolean", str)
		b = false
	}
	return b
}

func getDatabaseColumn(structField reflect.StructField) DatabaseColumn {
	if _, ok := structField.Tag.Lookup(TAG_DB_COLUMN); !ok {
		logger.Panicf("Field %q is missing a %q tag", structField.Name, TAG_DB_COLUMN)
		panic(structField.Name)
	}

	dbColumn := DatabaseColumn{
		name:     structField.Tag.Get(TAG_DB_COLUMN),
		dataType: getDatabaseColumnDatatype(structField),
		primary:  getBool(structField.Tag.Get(TAG_PRIMARY)),
		index:    getBool(structField.Tag.Get(TAG_INDEX)),
	}

	// By default, an org. ID is going to be a part of a primary key
	if dbColumn.name == ORG_ID_COLUMN_NAME {
		dbColumn.primary = true
	}
	return dbColumn
}

func getDatabaseColumnDatatype(structField reflect.StructField) string {
	switch structField.Type.Kind() {
	case reflect.String:
		return "TEXT"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "INTEGER"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Slice: // TODO: Check for uint8 slices only
		return "BYTEA"
	default:
		logger.Panicf("Unsupported type %v in %v", structField.Type.Kind(), structField)
		panic(structField.Name)
	}
}

/*
Wraps IF NOT EXISTS around the given statement
*/
func addIfNotExists(stmt, cond string) string {
	var wrapper strings.Builder
	wrapper.WriteString("DO\n")
	wrapper.WriteString("$$\n")
	wrapper.WriteString("BEGIN\n")
	wrapper.WriteString("\tIF NOT EXISTS (")
	wrapper.WriteString(cond)
	wrapper.WriteString(") THEN\n")
	wrapper.WriteString("\t\t")
	wrapper.WriteString(stmt)
	wrapper.WriteString(";\n")
	wrapper.WriteString("\tEND IF;\n")
	wrapper.WriteString("END\n")
	wrapper.WriteString("$$;")
	return wrapper.String()
}

func addIfExists(stmt, cond string) string {
	wrapper := addIfNotExists(stmt, cond)
	wrapper = strings.Replace(wrapper, "IF NOT EXISTS", "IF EXISTS", 1)
	return wrapper
}

/*
Returns a SQL statement that sets a Postgres config. parameter
settingName - parameter name
settingValue - parameter value
isLocal - if true, the new value for the parameter will apply only for current transaction; otherwise, the new value will be visible outside this transaction
*/
func getSetConfigStmt(settingName, settingValue string, isLocal bool) string {
	var stmt strings.Builder
	stmt.WriteString("SELECT set_config('")
	stmt.WriteString(settingName)
	stmt.WriteString("', '")
	stmt.WriteString(settingValue)
	stmt.WriteString("', ")
	stmt.WriteString(strconv.FormatBool(isLocal))
	stmt.WriteString(")")
	logger.Infof("Executing Statement %s", stmt.String())
	return stmt.String()
}

/*
Returns a SQL statement that enables row-level security in a DB table
*/
func getEnableRLSStmt(tableName string, x Record) string {
	var stmt strings.Builder
	stmt.WriteString("ALTER TABLE ")
	stmt.WriteString(tableName)
	stmt.WriteString(" ENABLE ROW LEVEL SECURITY;")
	logger.Infof("Executing Statement %s", stmt.String())
	return stmt.String()
}

/*
Returns a SQL statement that creates a DB user with the given specs, if it does not exist yet
*/
func getCreateUserStmt(username DbRole, password string) string {
	var findRoleQuery, createUserStmt strings.Builder
	findRoleQuery.WriteString("SELECT * FROM pg_roles WHERE rolname = '")
	findRoleQuery.WriteString(string(username))
	findRoleQuery.WriteString("'")

	createUserStmt.WriteString("CREATE USER ")
	createUserStmt.WriteString(string(username))
	createUserStmt.WriteString(" WITH PASSWORD '")
	createUserStmt.WriteString(password)
	createUserStmt.WriteString("'")

	stmt := addIfNotExists(createUserStmt.String(), findRoleQuery.String())
	sanitizedStmt := strings.ReplaceAll(stmt, password, "*******")
	logger.Infof("Executing Statement %s", sanitizedStmt)
	return stmt
}

func getGrantPrivilegesStmt(tableName string, x Record, username DbRole, commands []string) string {
	var stmt strings.Builder
	stmt.WriteString("GRANT ")
	stmt.WriteString(strings.Join(commands, ", "))
	stmt.WriteString(" ON ")
	stmt.WriteString(tableName)
	stmt.WriteString(" TO ")
	stmt.WriteString(string(username))
	stmt.WriteString(";")
	logger.Infof("Executing Statement %s", stmt.String())
	return stmt.String()
}

/*
Returns a SQL statement that creates an RLS-policy with the given specs, if it does not exist yet
*/
func getCreatePolicyStmt(tableName string, x Record, userSpecs dbUserSpecs) string {
	if userSpecs.existingRowsCond == "" || userSpecs.newRowsCond == "" {
		panic(userSpecs)
	}

	var createPolicyStmt, findPolicyQuery strings.Builder
	findPolicyQuery.WriteString("SELECT * FROM pg_policy WHERE polname = '")
	findPolicyQuery.WriteString(userSpecs.policyName)
	findPolicyQuery.WriteString("'")

	createPolicyStmt.WriteString("CREATE POLICY ")
	createPolicyStmt.WriteString(userSpecs.policyName)
	createPolicyStmt.WriteString(" ON ")
	createPolicyStmt.WriteString(tableName)
	createPolicyStmt.WriteString(" TO ")
	createPolicyStmt.WriteString(string(userSpecs.username))
	createPolicyStmt.WriteString("\n\t\t")
	createPolicyStmt.WriteString("USING (")
	createPolicyStmt.WriteString(userSpecs.existingRowsCond)
	createPolicyStmt.WriteString(")\n\t\t")
	createPolicyStmt.WriteString("WITH CHECK (")
	createPolicyStmt.WriteString(userSpecs.newRowsCond)
	createPolicyStmt.WriteString(")")

	stmt := addIfNotExists(createPolicyStmt.String(), findPolicyQuery.String())
	logger.Infof("Executing Statement %s", stmt)
	return stmt
}

/*
Validates that the given struct satisfied the following preconditions:
- Each field is exported
- Each field has a tag for DB column name with a unique value
- At least one field has a tag marking it as a primary key
- Only supported data types are used in the struct
*/
func validateStruct(x Record) {
	structValue := reflect.ValueOf(x)
	if structValue.Kind() == reflect.Ptr {
		structValue = structValue.Elem()
	}
	structType := structValue.Type()

	// Each field in the struct has to be exported, have a 'db_column' tag, be of one of the supported types
	//The struct has to have a primary key consisting of one field
	numPrimaryKeyTags := 0
	var dbColumnNamesSet = make(map[string]struct{}) // Set that will contain the [unique] names of DB columns'
	for i := 0; i < structType.NumField(); i++ {
		dbColumnNamesSet[structType.Field(i).Tag.Get(TAG_DB_COLUMN)] = struct{}{}

		if getBool(structType.Field(i).Tag.Get(TAG_PRIMARY)) {
			numPrimaryKeyTags++
		}

		if firstLetter := rune(structType.Field(i).Name[0]); !unicode.IsUpper(firstLetter) {
			panic(x)
		}
	}
	if numPrimaryKeyTags < 1 {
		logger.Panicf("The struct %q does not contain a field with %q tag", structType.Name(), TAG_PRIMARY)
		panic(structType.Name())
	}
	if structType.NumField() != len(dbColumnNamesSet) {
		logger.Panicf("The struct %q contains duplicate values for %s tag", structType.Name(), TAG_DB_COLUMN)
		panic(structType.Name())
	}
	if _, ok := dbColumnNamesSet[REVISION_COLUMN_NAME]; !ok {
		logger.Infof("The struct %q does not contain a field with %q tag", structType.Name(), REVISION_COLUMN_NAME)
	}
}

func getColumnNames(tableName string, x Record) *[]string {
	dbColumns := getDatabaseColumns(tableName, x)
	columnNames := make([]string, len(*dbColumns))
	for i := range *dbColumns {
		columnNames[i] = (*dbColumns)[i].name
	}

	return &columnNames
}

func getColumnNamesCommaSeparated(tableName string, x Record) string {
	columns := *getColumnNames(tableName, x)
	return strings.Join(columns, ", ")
}

func getDatabaseColumns(tableName string, x Record) *[]DatabaseColumn {
	structType := reflect.TypeOf(x)
	if structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
	}
	if _, ok := databaseColumns[tableName]; !ok {
		columns := make([]DatabaseColumn, structType.NumField())
		for i := 0; i < structType.NumField(); i++ {
			field := structType.Field(i)
			columns[i] = getDatabaseColumn(field)
		}
		databaseColumns[tableName] = &columns
	}
	return databaseColumns[tableName]
}

// Extracts struct's name, which will serve as DB table name, using reflection.
func GetTableName(x interface{}) string {
	var tableName string
	if reflect.TypeOf(x).Kind() == reflect.Ptr {
		tableName = reflect.ValueOf(x).Elem().Type().Name()
	} else {
		tableName = reflect.TypeOf(x).Name()
	}

	tableName = "\"" + tableName + "\""

	return tableName
}

/*
Extracts name of a struct comprising the input slice
*/
func GetTableNameFromSlice(x interface{}) string {
	var sliceType = reflect.TypeOf(x)
	if sliceType.Kind() == reflect.Ptr {
		sliceType = sliceType.Elem()
	}

	var sliceElemType = sliceType.Elem()
	if sliceElemType.Kind() == reflect.Ptr {
		sliceElemType = sliceElemType.Elem()
	}
	tableName := GetTableName(reflect.New(sliceElemType).Interface())
	return tableName
}

/*
Generates RLS-policy name based on database role/user and table name
*/
func GetRlsPolicyName(username DbRole, tableName string) string {
	policyName := strings.ToLower(string(username) + "_" + tableName + "_policy")
	policyName = strings.ReplaceAll(policyName, "\"", "")
	return policyName
}

/*
Returns a CREATE TABLE statement.
Panics if the record does not satisfy certain preconditions (see validateStruct()).
*/
func getCreateTableStmt(tableName string, x Record) string {
	// Validate the struct; panic if some requirements are not met
	validateStruct(x)

	var stmt strings.Builder
	var primaryKeys = make([]string, 0, 4 /* init. capacity */)
	stmt.WriteString("CREATE TABLE IF NOT EXISTS ")
	stmt.WriteString(tableName)
	stmt.WriteString(" (\n")
	for _, col := range *getDatabaseColumns(tableName, x) {
		stmt.WriteString("\t")
		stmt.WriteString(col.name)
		stmt.WriteString(" ")
		stmt.WriteString(col.dataType)
		stmt.WriteString(",\n")
		if col.primary {
			primaryKeys = append(primaryKeys, col.name)
		}
	}
	stmt.WriteString("\tPRIMARY KEY(")
	stmt.WriteString(strings.Join(primaryKeys, ", "))
	stmt.WriteString(")")
	stmt.WriteString("\n);")
	logger.Infof("Executing Statement %s", stmt.String())
	return stmt.String()
}

func getRevisionTriggerName(tableName string) string {
	return strings.ReplaceAll(tableName, "\"", "") + "_revision_trigger"
}

func getDropIfExistsRevisionTriggerStmt(tableName string) string {
	var stmt strings.Builder
	stmt.WriteString("DROP TRIGGER IF EXISTS ")
	stmt.WriteString(getRevisionTriggerName(tableName))
	stmt.WriteString(" ON ")
	stmt.WriteString(tableName)
	stmt.WriteString(" RESTRICT;")
	logger.Infof("Executing Statement %s", stmt.String())
	return stmt.String()
}

func getCreateRevisionTriggerStmt(tableName string) string {
	var stmt strings.Builder
	stmt.WriteString("CREATE TRIGGER ")
	stmt.WriteString(getRevisionTriggerName(tableName))
	stmt.WriteString(" BEFORE UPDATE\n")
	stmt.WriteString("\t ON ")
	stmt.WriteString(tableName)
	stmt.WriteString("\n\tFOR EACH ROW\n")
	stmt.WriteString("\tEXECUTE FUNCTION check_and_update_revision();")
	logger.Infof("Executing Statement %s", stmt.String())
	return stmt.String()
}

func getCreateRevisionFunctionStmt() string {
	var stmt strings.Builder
	stmt.WriteString("CREATE OR REPLACE FUNCTION check_and_update_revision() RETURNS TRIGGER AS $$\n")
	stmt.WriteString("\tBEGIN\n")
	stmt.WriteString("\t\tIF NEW.")
	stmt.WriteString(REVISION_COLUMN_NAME)
	stmt.WriteString(" = OLD.")
	stmt.WriteString(REVISION_COLUMN_NAME)
	stmt.WriteString(" THEN\n")
	stmt.WriteString("\t\t\tNEW.")
	stmt.WriteString(REVISION_COLUMN_NAME)
	stmt.WriteString(" := OLD.")
	stmt.WriteString(REVISION_COLUMN_NAME)
	stmt.WriteString(" + 1;\n")
	stmt.WriteString("\t\t\tRETURN NEW;\n")
	stmt.WriteString("\t\tELSE\n")
	stmt.WriteString("\t\t\tRAISE EXCEPTION '")
	stmt.WriteString(REVISION_OUTDATED_MSG)
	stmt.WriteString(REVISION_COLUMN_NAME)
	stmt.WriteString(": %', NEW.")
	stmt.WriteString(REVISION_COLUMN_NAME)
	stmt.WriteString(";\n")
	stmt.WriteString("\t\tEND IF;\n")
	stmt.WriteString("\tEND;\n")
	stmt.WriteString("$$ LANGUAGE PLPGSQL;")
	logger.Infof("Executing Statement %s", stmt.String())
	return stmt.String()
}

func getTruncateTableStmt(tableName string) string {
	var findTableQuery, truncateTableStmt strings.Builder

	findTableQuery.WriteString("SELECT * FROM pg_tables WHERE schemaname = 'public' AND tablename = '")
	findTableQuery.WriteString(strings.ReplaceAll(tableName, "\"", ""))
	findTableQuery.WriteString("'")

	truncateTableStmt.WriteString("TRUNCATE TABLE ")
	truncateTableStmt.WriteString(tableName)

	return addIfExists(truncateTableStmt.String(), findTableQuery.String())
}

// Returns a DROP TABLE statement
func getDropTableStmt(tableName string) string {
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s ;", tableName)
	logger.Infof("Executing Statement %s", stmt)
	return stmt
}

// Returns a SELECT statement with no WHERE condition(s) and arguments to be used in place of placeholders.
// Considers non-empty fields of x as filters for WHERE clause
func getSelectStmt(tableName string, x Record) (string, []interface{}) {
	var placeholderIndex = 1
	structValue := reflect.ValueOf(x)
	if structValue.Kind() == reflect.Ptr {
		structValue = structValue.Elem()
	}
	structType := structValue.Type()
	args := make([]interface{}, 0, structValue.NumField())

	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(getColumnNamesCommaSeparated(tableName, x))
	query.WriteString("\nFROM ")
	query.WriteString(tableName)

	// Prepare WHERE clause
	conditions := make([]string, 0, structValue.NumField())
	for i := 0; i < structValue.NumField(); i++ {
		if field := structValue.Field(i); !field.IsZero() {
			conditions = append(conditions, structType.Field(i).Tag.Get(TAG_DB_COLUMN)+" = $"+strconv.Itoa(placeholderIndex))
			args = append(args, getUnderlyingValueBasedOnKind(field.Kind(), field))
			placeholderIndex++
		}
	}
	if len(args) > 0 {
		query.WriteString("\nWHERE ")
		query.WriteString(strings.Join(conditions, " AND "))
	}

	query.WriteString("\nORDER BY ")
	query.WriteString(strings.Join(getPrimaryKey(tableName, x), ", "))
	return query.String(), args
}

// Returns a SELECT statement with a single WHERE condition for the primary key
func getSelectSingleStmt(tableName string, x Record) string {

	placeholderIndex := 1
	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(getColumnNamesCommaSeparated(tableName, x))
	query.WriteString("\nFROM ")
	query.WriteString(tableName)
	query.WriteString("\nWHERE ")
	for i, primaryKey := 0, getPrimaryKey(tableName, x); i < len(primaryKey); i++ {
		query.WriteString(primaryKey[i])
		query.WriteString(" = $")
		query.WriteString(strconv.Itoa(placeholderIndex))
		placeholderIndex++

		// Do not add a comma after the last column
		if i != len(primaryKey)-1 {
			query.WriteString(" AND ")
		}
	}

	return query.String()
}

func getSelectJoinStmt(table1Name string, table2Name string, record1 Record, record2 Record, record2JoinOnColumn string, limit int) string {

	var query strings.Builder
	table1Columns := *getColumnNames(table1Name, record1)
	table2Columns := *getColumnNames(table2Name, record2)

	// SELECT *
	query.WriteString("SELECT ")
	for tableNames, tables, tableIndex := []string{table1Name, table2Name}, [][]string{table1Columns, table2Columns}, 0; tableIndex < len(tableNames); tableIndex++ {
		tableColumns := tables[tableIndex]
		tableName := tableNames[tableIndex]

		for columnIndex, columnName := range tableColumns {
			query.WriteString(tableName)
			query.WriteString(".")
			query.WriteString(columnName)
			if !(tableIndex == 1 && columnIndex == len(tableColumns)-1) { // Do not add a comma if it's the last column
				query.WriteString(", ")
			}
		}
	}

	// FROM %s
	query.WriteString("\nFROM ")
	query.WriteString(table1Name)

	// INNER JOIN %s
	query.WriteString(" INNER JOIN ")
	query.WriteString(table2Name)

	// ON %s.%s=%s.%s
	query.WriteString(" ON ")
	query.WriteString(table1Name)
	query.WriteString(".")
	query.WriteString(getPrimaryKey(table1Name, record1)[0])
	query.WriteString("=")
	query.WriteString(table2Name)
	query.WriteString(".")
	query.WriteString(record2JoinOnColumn)

	// WHERE %s.%s = $1
	placeholderIndex := 1
	query.WriteString("\nWHERE ")
	query.WriteString(table1Name)
	query.WriteString(".")
	query.WriteString(getPrimaryKey(table1Name, record1)[0])
	query.WriteString(" = $")
	query.WriteString(strconv.Itoa(placeholderIndex))

	// ORDER BY %s.%s, %s.%s
	query.WriteString("\nORDER BY ")
	query.WriteString(table1Name)
	query.WriteString(".")
	query.WriteString(getPrimaryKey(table1Name, record1)[0])
	query.WriteString(", ")
	query.WriteString(table2Name)
	query.WriteString(".")
	query.WriteString(getPrimaryKey(table2Name, record2)[0])

	// LIMIT %d
	if limit > 0 {
		query.WriteString("\nLIMIT ")
		query.WriteString(strconv.Itoa(limit))
	}

	return query.String()
}

/*
Returns an UPDATE statement and a list of arguments for the prepared statement.
*/
func getUpdateStmt(tableName string, x Record) (string, []interface{}) {
	structValue := reflect.ValueOf(x)
	if structValue.Kind() == reflect.Ptr {
		structValue = structValue.Elem()
	}
	structType := structValue.Type()

	var stmt strings.Builder
	stmt.WriteString("UPDATE ")
	stmt.WriteString(tableName)
	stmt.WriteString(" SET ")

	placeholderIndex := 1
	args := make([]interface{}, 0, structType.NumField()) // Placeholders parameters for prepared statement

	nonPrimaryKeyColumns := make([]string, 0, 10 /* init. capacity */)
	for i := 0; i < structType.NumField(); i++ {
		// Check if current column is part of a primary key. If yes, do not modify it
		columnName := structType.Field(i).Tag.Get(TAG_DB_COLUMN)
		primaryKey := getPrimaryKey(tableName, x)
		if index := sort.SearchStrings(primaryKey, columnName); index < len(primaryKey) && primaryKey[index] == columnName {
			continue
		}

		nonPrimaryKeyColumns = append(nonPrimaryKeyColumns, columnName+" = $"+strconv.Itoa(placeholderIndex))
		args = append(args, getUnderlyingValueBasedOnKind(structValue.Field(i).Kind(), structValue.Field(i)))
		placeholderIndex++
	}
	stmt.WriteString(strings.Join(nonPrimaryKeyColumns, ", "))

	stmt.WriteString(" WHERE ")
	for i, primaryKey := 0, getPrimaryKey(tableName, x); i < len(primaryKey); i++ {
		stmt.WriteString(primaryKey[i])
		stmt.WriteString(" = $")
		stmt.WriteString(strconv.Itoa(placeholderIndex))
		placeholderIndex++

		// Do not add a comma after the last column
		if i != len(primaryKey)-1 {
			stmt.WriteString(" AND ")
		}
	}
	args = append(args, x.GetId()...)

	return stmt.String(), args
}

func getUpsertStmt(tableName string, x Record) (string, []interface{}) {
	structValue := reflect.ValueOf(x)
	if structValue.Kind() == reflect.Ptr {
		structValue = structValue.Elem()
	}
	structType := structValue.Type()

	var stmt strings.Builder
	stmt.WriteString("INSERT INTO ")
	stmt.WriteString(tableName)
	stmt.WriteString(" (")
	stmt.WriteString(getColumnNamesCommaSeparated(tableName, x))
	stmt.WriteString(")\n")
	stmt.WriteString("VALUES (")

	placeholderIndex := 1
	args := make([]interface{}, 0, structValue.NumField()) // Placeholders parameters for prepared statement
	for i := 0; i < structValue.NumField(); i++ {
		stmt.WriteString("$")
		stmt.WriteString(strconv.Itoa(placeholderIndex))
		args = append(args, getUnderlyingValueBasedOnKind(structValue.Field(i).Kind(), structValue.Field(i)))
		placeholderIndex++
		if i != structValue.NumField()-1 {
			stmt.WriteString(", ")
		}
	}
	stmt.WriteString(")\n")
	stmt.WriteString("ON CONFLICT (")
	primaryKeyColumns := make([]string, 0, 10 /* init. capacity */)
	for i := 0; i < structType.NumField(); i++ {
		columnName := structType.Field(i).Tag.Get(TAG_DB_COLUMN)
		primaryKey := getPrimaryKey(tableName, x)
		if index := sort.SearchStrings(primaryKey, columnName); index < len(primaryKey) && primaryKey[index] == columnName {
			primaryKeyColumns = append(primaryKeyColumns, columnName)
		}
	}
	stmt.WriteString(strings.Join(primaryKeyColumns, ", "))
	stmt.WriteString(")\n")
	stmt.WriteString("DO UPDATE SET ")

	nonPrimaryKeyColumns := make([]string, 0, 10 /* init. capacity */)
	for i := 0; i < structType.NumField(); i++ {
		// Check if current column is part of a primary key. If yes, do not modify it
		columnName := structType.Field(i).Tag.Get(TAG_DB_COLUMN)
		primaryKey := getPrimaryKey(tableName, x)
		if index := sort.SearchStrings(primaryKey, columnName); index < len(primaryKey) && primaryKey[index] == columnName {
			continue
		}

		nonPrimaryKeyColumns = append(nonPrimaryKeyColumns, columnName+" = $"+strconv.Itoa(placeholderIndex))
		args = append(args, getUnderlyingValueBasedOnKind(structValue.Field(i).Kind(), structValue.Field(i)))
		placeholderIndex++
	}

	stmt.WriteString(strings.Join(nonPrimaryKeyColumns, ", "))

	return stmt.String(), args
}

// Returns a DELETE statement.
func getDeleteStmt(tableName string, x Record) string {

	placeholderIndex := 1
	var stmt strings.Builder
	stmt.WriteString("DELETE FROM ")
	stmt.WriteString(tableName)
	stmt.WriteString(" WHERE ")
	for i, primaryKey := 0, getPrimaryKey(tableName, x); i < len(primaryKey); i++ {
		stmt.WriteString(primaryKey[i])
		stmt.WriteString(" = $")
		stmt.WriteString(strconv.Itoa(placeholderIndex))
		placeholderIndex++

		// Do not add a comma after the last column
		if i != len(primaryKey)-1 {
			stmt.WriteString(" AND ")
		}
	}

	return stmt.String()
}

// Returns a prepared INSERT statement and a list of arguments for the prepared statement.
func getInsertStmt(tableName string, x Record) (string, []interface{}) {
	structValue := reflect.ValueOf(x)
	if structValue.Kind() == reflect.Ptr {
		structValue = structValue.Elem()
	}
	var stmt strings.Builder
	stmt.WriteString("INSERT INTO ")
	stmt.WriteString(tableName)
	stmt.WriteString(" (")
	stmt.WriteString(getColumnNamesCommaSeparated(tableName, x))
	stmt.WriteString(") ")
	stmt.WriteString(" VALUES (")

	placeholderIndex := 1
	args := make([]interface{}, 0, structValue.NumField()) // Placeholders parameters for prepared statement
	for i := 0; i < structValue.NumField(); i++ {
		stmt.WriteString("$")
		stmt.WriteString(strconv.Itoa(placeholderIndex))
		args = append(args, getUnderlyingValueBasedOnKind(structValue.Field(i).Kind(), structValue.Field(i)))
		placeholderIndex++
		if i != structValue.NumField()-1 {
			stmt.WriteString(", ")
		}
	}
	stmt.WriteString(")")
	return stmt.String(), args
}

/*
Replaces placeholders in a SQL statement, such as $1, $2, etc., with actual values.
Should be used only for logging purposes. For all other purposes, prepared statements should always be used
instead of replacing placeholders manually.
*/
func replacePlaceholdersInSqlStmt(sqlStmt string, args ...interface{}) string {
	if strings.Count(sqlStmt, "$") != len(args) {
		logger.Warnf("Not enough/too many arguments to replace the placeholders in SQL statement %q", sqlStmt)
		return sqlStmt
	}

	// Replace placeholders
	for i, arg := range args {
		index := i + 1 // Placeholder's index that follows $
		sqlStmt = strings.Replace(sqlStmt, "$"+strconv.Itoa(index), fmt.Sprint(arg), 1)
	}

	logger.Debugf("SQL Statement: %v", sqlStmt)
	return sqlStmt
}

func validateIdAndRecordPtr(id string, record Record) error {
	return validateFieldAndRecordPtr(id, "id", record)
}

func validateFieldAndRecordPtr(field string, fieldName string, record Record) error {
	if len(strings.TrimSpace(field)) == 0 {
		errMsg := fmt.Sprintf("Argument %q cannot be blank", fieldName)
		logger.Error(errMsg)
		err := IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}

	if reflect.TypeOf(record).Kind() != reflect.Ptr {
		errMsg := "\"record\" argument has to be a pointer to a struct implementing \"Record\" interface"
		logger.Error(errMsg)
		err := IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}
	return nil
}

func validateFieldAndRecordSlicePtr(field string, fieldName string, slice interface{}, size int) error {
	if len(strings.TrimSpace(field)) == 0 {
		errMsg := fmt.Sprintf("Argument %q cannot be blank", fieldName)
		logger.Error(errMsg)
		err := IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}

	sliceType := reflect.TypeOf(slice)
	if sliceType.Kind() != reflect.Ptr {
		errMsg := "\"slice\" argument has to be a pointer to a slice of structs implementing \"Record\" interface"
		logger.Error(errMsg)
		err := IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}

	sliceValue := reflect.ValueOf(slice).Elem()
	if sliceValue.Kind() != reflect.Slice {
		errMsg := "\"slice\" argument has to be a pointer to a slice of structs implementing \"Record\" interface"
		logger.Error(errMsg)
		err := IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}

	if sliceValue.Len() != size {
		errMsg := fmt.Sprintf("\"slice\" argument has to have a size of %d", size)
		logger.Error(errMsg)
		err := IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}

	return nil
}

func validateIdAndRecordStruct(id string, record Record) error {
	if len(strings.TrimSpace(id)) == 0 {
		errMsg := "\"id\" cannot be blank"
		logger.Error(errMsg)
		err := IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}

	if reflect.TypeOf(record).Kind() != reflect.Struct {
		errMsg := "\"record\" argument has to be a struct implementing \"Record\" interface"
		logger.Error(errMsg)
		err := IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}
	return nil
}

func getInstanceFromKind(kind reflect.Kind) interface{} {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return new(int)
	case reflect.Int64:
		return new(int64)
	case reflect.String:
		return new(string)
	case reflect.Bool:
		return new(bool)
	}
	return new(string)
}

func getUnderlyingValueBasedOnKind(kind reflect.Kind, value reflect.Value) interface{} {
	switch kind {
	case reflect.String:
		return value.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int()
	case reflect.Bool:
		return value.Bool()
	case reflect.Slice:
		return value.Bytes()
	default:
		logger.Panicf("Unsupported type %v", kind)
		panic(kind.String())
	}
}

func updateValueBasedOnKind(field reflect.Value, from interface{}) {
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		field.SetInt(int64(*from.(*int)))
	case reflect.Int64:
		field.SetInt(*from.(*int64))
	case reflect.String:
		field.SetString(*from.(*string))
	case reflect.Bool:
		field.SetBool(*from.(*bool))
	case reflect.Slice:
		field.SetBytes([]byte(*from.(*string)))
	default:
		logger.Panicf("Unhandled Kind: %v", field.Kind())
		panic(field.Kind())
	}
}

/*
Returns the requested field's value from record, which is a struct or a pointer to a struct implementing Record interface.
Uses a tag rather than field name to find the desired field.
Panics if such a field is not present.
*/
func getField(fieldName string, record Record) string {
	if field, ok := getFieldNoPanic(fieldName, record); ok {
		return field
	} else {
		logger.Panicf("Could not find field with tag %q equal to %q in struct %v", TAG_DB_COLUMN, fieldName, GetTableName(record))
		panic(GetTableName(record))
	}
}

/*
Returns the requested field's value from record, which is a struct or a pointer to a struct implementing Record interface.
Uses a tag rather than field name to find the desired field.
Returns an empty string and false if such a field is not present.
*/
func getFieldNoPanic(fieldName string, record Record) (string, bool) {
	structValue := reflect.ValueOf(record)
	if structValue.Kind() == reflect.Ptr {
		structValue = structValue.Elem()
	}
	structType := structValue.Type()

	for i := 0; i < structType.NumField(); i++ {
		if structType.Field(i).Tag.Get(TAG_DB_COLUMN) == fieldName {
			return structValue.Field(i).String(), true
		}
	}

	return "", false
}
