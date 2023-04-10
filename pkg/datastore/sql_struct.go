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
	"reflect"
	"strings"
	"sync"

	"gorm.io/gorm/schema"
)

// Library processing all the golang tags in the struct into SQL domain

const (
	// Struct Field Names.
	FIELD_ORGID      = "OrgId"
	FIELD_INSTANCEID = "InstanceId"

	// SQL Columns.
	COLUMN_ORGID      = "org_id"
	COLUMN_INSTANCEID = "instance_id"
	COLUMN_REVISION   = "revision"

	// Messages.
	REVISION_OUTDATED_MSG = "Invalid update - outdated "
)

var (
	// Maps table names to their corresponding feature booleans indicating whether each table supports multi-tenant, multi-instance and revisioning.
	schemaMap = map[string]map[string]bool{}
	// Maps struct types to their corresponding table names generated by schema.Parse.
	tableNameMap = map[string]string{}
)

func cacheSchemaSpec(tableName string, s *schema.Schema) {
	schemaMap[tableName] = make(map[string]bool)
	_, ok := s.FieldsByDBName[COLUMN_REVISION]
	schemaMap[tableName][COLUMN_REVISION] = ok
	_, ok = s.FieldsByDBName[COLUMN_ORGID]
	schemaMap[tableName][COLUMN_ORGID] = ok
	_, ok = s.FieldsByDBName[COLUMN_INSTANCEID]
	schemaMap[tableName][COLUMN_INSTANCEID] = ok
}

// Checks if revisioning is supported in the given table.
func IsRevisioned(x Record, tableName string) bool {
	return IsColumnPresent(x, tableName, COLUMN_REVISION)
}

// Checks if multiple tenants are supported in the given table.
func IsMultiTenanted(x Record, tableName string) bool {
	return IsColumnPresent(x, tableName, COLUMN_ORGID)
}

// Checks if multiple deployment instances are supported in the given table.
func IsMultiInstanced(x Record, tableName string, instancerConfigured bool) bool {
	return instancerConfigured && IsColumnPresent(x, tableName, COLUMN_INSTANCEID)
}

// Row Level Security to used to partition tables for multi-tenancy and multi-instance support.
func IsRowLevelSecurityRequired(record Record, tableName string, instancerConfigured bool) bool {
	return IsMultiTenanted(record, tableName) || IsMultiInstanced(record, tableName, instancerConfigured)
}

func IsColumnPresent(x Record, tableName, columnName string) bool {
	if _, ok := schemaMap[tableName]; !ok {
		s, _ := schemaParse(x)
		cacheSchemaSpec(tableName, s)
	}
	return schemaMap[tableName][columnName]
}

// Wraps IF NOT EXISTS around the given statement.
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

// Returns a SQL statement that sets a Postgres config. parameter
// settingName - parameter name
// settingValue - parameter value.
func getSetConfigStmt(settingName, settingValue string) string {
	var stmt strings.Builder
	stmt.WriteString("SET ")
	stmt.WriteString(settingName)
	stmt.WriteString("='")
	stmt.WriteString(settingValue)
	stmt.WriteString("';")
	TRACE("[SQL] %s", stmt.String())
	return stmt.String()
}

// Returns a SQL statement that enables row-level security in a DB table.
func getEnableRLSStmt(tableName string, _ Record) string {
	var stmt strings.Builder
	stmt.WriteString("ALTER TABLE ")
	stmt.WriteString(tableName)
	stmt.WriteString(" ENABLE ROW LEVEL SECURITY;")
	TRACE("[SQL] %s", stmt.String())
	return stmt.String()
}

// Returns a SQL statement that creates a DB user with the given specs, if it does not exist yet.
func getCreateUserStmt(username string, password string) string {
	var findRoleQuery, createUserStmt strings.Builder
	findRoleQuery.WriteString("SELECT * FROM pg_roles WHERE rolname = '")
	findRoleQuery.WriteString(username)
	findRoleQuery.WriteString("'")

	createUserStmt.WriteString("CREATE USER ")
	createUserStmt.WriteString(username)
	createUserStmt.WriteString(" WITH PASSWORD '")
	createUserStmt.WriteString(password)
	createUserStmt.WriteString("'")

	stmt := addIfNotExists(createUserStmt.String(), findRoleQuery.String())
	sanitizedStmt := strings.ReplaceAll(stmt, password, "*******")
	TRACE("[SQL] %s", sanitizedStmt)
	return stmt
}

func getGrantPrivilegesStmt(tableName string, username string, commands []string) string {
	var stmt strings.Builder
	stmt.WriteString("GRANT ")
	stmt.WriteString(strings.Join(commands, ", "))
	stmt.WriteString(" ON ")
	stmt.WriteString(tableName)
	stmt.WriteString(" TO ")
	stmt.WriteString(username)
	stmt.WriteString(";")
	TRACE("[SQL] %s", stmt.String())
	return stmt.String()
}

// Returns a SQL statement that creates an RLS-policy with the given specs, if it does not exist yet.
func getCreatePolicyStmt(tableName string, _ Record, dbUser dbUserSpec) string {
	if dbUser.existingRowsCond == "" || dbUser.newRowsCond == "" {
		panic(dbUser)
	}

	var createPolicyStmt, findPolicyQuery strings.Builder
	findPolicyQuery.WriteString("SELECT * FROM pg_policy WHERE polname = '")
	findPolicyQuery.WriteString(dbUser.policyName)
	findPolicyQuery.WriteString("'")

	createPolicyStmt.WriteString("CREATE POLICY ")
	createPolicyStmt.WriteString(dbUser.policyName)
	createPolicyStmt.WriteString(" ON ")
	createPolicyStmt.WriteString(tableName)
	createPolicyStmt.WriteString(" TO ")
	createPolicyStmt.WriteString(string(dbUser.username))
	createPolicyStmt.WriteString("\n\t\t")
	createPolicyStmt.WriteString("USING (")
	createPolicyStmt.WriteString(dbUser.existingRowsCond)
	createPolicyStmt.WriteString(")\n\t\t")
	createPolicyStmt.WriteString("WITH CHECK (")
	createPolicyStmt.WriteString(dbUser.newRowsCond)
	createPolicyStmt.WriteString(")")

	stmt := addIfNotExists(createPolicyStmt.String(), findPolicyQuery.String())
	TRACE("[SQL] %s", stmt)
	return stmt
}

func typeName(x interface{}, prefix string) string {
	t := reflect.TypeOf(x)
	TRACE("type for %+v is %s\n", x, t.Kind())
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
		prefix += "*"
	}
	for t.Kind() == reflect.Slice {
		t = t.Elem()
		prefix += "[]"
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
		prefix += "*"
	}
	return prefix + t.Name()
}

func TypeName(x interface{}) string {
	return typeName(x, "")
}

func IsPointerToStruct(x interface{}) (isPtrType bool) {
	t := reflect.TypeOf(x)
	isPtrType = t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct
	return
}

func schemaParse(x interface{}) (*schema.Schema, error) {
	TRACE("Parsing schema for %s, %+v", TypeName(x), x)
	s, err := schema.Parse(x, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		TRACE("Unable to parse schema: %s, %+v, %+v", TypeName(x), x, err)
	}
	return s, err
}

// Extracts struct's name, which will serve as DB table name, using reflection.
func GetTableName(x interface{}) (tableName string) {
	if t, ok := x.(schema.Tabler); ok {
		tableName = t.TableName()
	} else {
		typ := TypeName(x)
		if _, ok := tableNameMap[typ]; !ok {
			s, _ := schemaParse(x)
			tableNameMap[typ] = s.Table
		}
		tableName = tableNameMap[typ]
	}
	TRACE("TableName Map is %+v\n", tableNameMap)
	TRACE("TableName is %s for %+v\n", tableName, x)
	return tableName
}

// Generates RLS-policy name based on database role/user and table name.
func getRlsPolicyName(username string, tableName string) string {
	policyName := strings.ToLower(username + "_" + tableName + "_policy")
	policyName = strings.ReplaceAll(policyName, "\"", "")
	return policyName
}

// Constructs a trigger name that will include the name of table where trigger
// is going to be applied on and the name of a function that will be executed
// by trigger.
func getTriggerName(tableName, functionName string) string {
	functionName = strings.TrimSuffix(functionName, "()")
	tableName = strings.ReplaceAll(tableName, "\"", "")
	return strings.Join([]string{"\"", tableName, "_", functionName, "_trigger\""}, "")
}

func getDropTriggerStmt(tableName, functionName string) string {
	var stmt strings.Builder
	stmt.WriteString("DROP TRIGGER IF EXISTS ")
	stmt.WriteString(getTriggerName(tableName, functionName))
	stmt.WriteString(" ON ")
	stmt.WriteString(tableName)
	stmt.WriteString(" RESTRICT;")
	TRACE("[SQL] %s", stmt.String())
	return stmt.String()
}

// Returns PL/pgSQL statement that creates a trigger invoking the given function.
func getCreateTriggerStmt(tableName, functionName string) string {
	if !strings.HasSuffix(functionName, "()") {
		functionName += "()"
	}

	var stmt strings.Builder
	stmt.WriteString("CREATE TRIGGER ")
	stmt.WriteString(getTriggerName(tableName, functionName))
	stmt.WriteString(" BEFORE UPDATE\n")
	stmt.WriteString("\t ON ")
	stmt.WriteString(tableName)
	stmt.WriteString("\n\tFOR EACH ROW\n")
	stmt.WriteString("\tEXECUTE FUNCTION ")
	stmt.WriteString(functionName)

	TRACE("[SQL] %s", stmt.String())
	return stmt.String()
}

// Returns a PL/pgSQL function that does the following:
//
//	Rejects updates where a new revision does not equal the current one
//	If an update is not rejected, increments revision by 1
//
//	IF NEW.revision = OLD.revision THEN
//		NEW.revision := OLD.revision + 1;
//		RETURN NEW;
//	ELSE
//		RAISE EXCEPTION 'Invalid update - outdated revision: %', NEW.revision;
//
//	END IF;.
func getCheckAndUpdateRevisionFunc() (functionName, functionBody string) {
	var stmt strings.Builder
	stmt.WriteString("\t\tIF NEW.")
	stmt.WriteString(COLUMN_REVISION)
	stmt.WriteString(" = OLD.")
	stmt.WriteString(COLUMN_REVISION)
	stmt.WriteString(" THEN\n")
	stmt.WriteString("\t\t\tNEW.")
	stmt.WriteString(COLUMN_REVISION)
	stmt.WriteString(" := OLD.")
	stmt.WriteString(COLUMN_REVISION)
	stmt.WriteString(" + 1;\n")
	stmt.WriteString("\t\t\tRETURN NEW;\n")
	stmt.WriteString("\t\tELSE\n")
	stmt.WriteString("\t\t\tRAISE EXCEPTION '")
	stmt.WriteString(REVISION_OUTDATED_MSG)
	stmt.WriteString(COLUMN_REVISION)
	stmt.WriteString(": %', NEW.")
	stmt.WriteString(COLUMN_REVISION)
	stmt.WriteString(";\n")
	stmt.WriteString("\t\tEND IF;")
	return "check_and_update_revision", stmt.String()
}

func getCreateTriggerFunctionStmt(functionName, functionBody string) string {
	if !strings.HasSuffix(functionName, "()") {
		functionName += "()"
	}
	var stmt strings.Builder
	stmt.WriteString("CREATE OR REPLACE FUNCTION ")
	stmt.WriteString(functionName)
	stmt.WriteString(" RETURNS TRIGGER AS $$\n")
	stmt.WriteString("\tBEGIN\n")
	stmt.WriteString(functionBody)
	stmt.WriteString("\n\tEND;\n")
	stmt.WriteString("$$ LANGUAGE PLPGSQL;")
	TRACE("[SQL] %s", stmt.String())
	return stmt.String()
}

func getFindTableStmt(tableName string) string {
	var findTableQuery strings.Builder

	findTableQuery.WriteString("SELECT * FROM pg_tables WHERE schemaname = 'public' AND tablename = '")
	findTableQuery.WriteString(strings.ReplaceAll(tableName, "\"", ""))
	findTableQuery.WriteString("'")

	return findTableQuery.String()
}

func getTruncateTableStmt(tableName string, cascade bool) string {
	findTableStmt := getFindTableStmt(tableName)

	var truncateTableStmt strings.Builder
	truncateTableStmt.WriteString("TRUNCATE TABLE ")
	truncateTableStmt.WriteString(tableName)
	if cascade {
		truncateTableStmt.WriteString(" CASCADE")
	}

	return addIfExists(truncateTableStmt.String(), findTableStmt)
}

// Returns the requested OrgId field's value from record, which is a pointer
// to a struct implementing Record interface.  Uses a tag rather than field
// name to find the desired field. Returns an empty string and false if such
// a field is not present.
func GetOrgId(record Record) (string, bool) {
	return GetFieldValue(record, FIELD_ORGID, COLUMN_ORGID)
}

// Returns the requested InstanceId field's value from record, which is a pointer
// to a struct implementing Record interface.  Uses a tag rather than field
// name to find the desired field. Returns an empty string and false if such
// a field is not present.
func GetInstanceId(record Record) (string, bool) {
	return GetFieldValue(record, FIELD_INSTANCEID, COLUMN_INSTANCEID)
}

func SetInstanceId(record Record, value string) bool {
	return SetFieldValue(record, FIELD_INSTANCEID, COLUMN_INSTANCEID, value)
}

// Returns the requested fields value from record, which is a pointer
// to a struct implementing Record interface.  Uses a tag rather than field
// name to find the desired field. Returns an empty string and false if such
// a field is not present.
func GetFieldValue(record Record, fieldName, columnName string) (string, bool) {
	if s, err := schemaParse(record); err == nil {
		if f, ok := s.FieldsByDBName[columnName]; ok {
			fieldName = f.Name
		}
	}
	structValue := reflect.ValueOf(record)
	if structValue.Kind() == reflect.Ptr {
		structValue = structValue.Elem()
	}
	structType := structValue.Type()

	for i := 0; i < structType.NumField(); i++ {
		if structType.Field(i).Name == fieldName {
			return structValue.Field(i).String(), true
		}
	}

	return "", false
}

func SetFieldValue(record Record, fieldName, columnName, value string) bool {
	if s, err := schemaParse(record); err == nil {
		if f, ok := s.FieldsByDBName[columnName]; ok {
			fieldName = f.Name
		}
	}
	structValue := reflect.ValueOf(record)
	if structValue.Kind() == reflect.Ptr {
		structValue = structValue.Elem()
	}
	structType := structValue.Type()

	for i := 0; i < structType.NumField(); i++ {
		if structType.Field(i).Name == fieldName {
			structValue.FieldByName(fieldName).SetString(value)
			return true
		}
	}

	return false
}
