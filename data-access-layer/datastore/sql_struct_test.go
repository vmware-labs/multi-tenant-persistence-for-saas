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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectStmtGeneration(t *testing.T) {
	assert := assert.New(t)
	myApp, _, _ := prepareInput()
	tableName := GetTableName(myApp)

	// Since myApp is non-empty, the generated SELECT statement must contain placeholders in WHERE clause
	stmt, args := getSelectStmt(tableName, myApp)
	assert.Equal(len(args), strings.Count(stmt, "$"))

	assert.Contains(stmt, "WHERE")
	// SELECT stmt must contain as many placeholders ('$') as there are non-empty fields in myApp
	assert.Equal(3, strings.Count(stmt, "$"))
	assert.NotEmpty(args)

	// Since the record passed is empty, the generated SELECT statement must NOT contain placeholders
	stmt, args = getSelectStmt(tableName, app{})
	assert.NotContains(stmt, "$")
	assert.NotContains(stmt, "WHERE")
	assert.Empty(args)
}

func TestSelectSingleStmtGeneration(t *testing.T) {
	assert := assert.New(t)
	myApp, _, _ := prepareInput()
	tableName := GetTableName(myApp)

	stmt := getSelectSingleStmt(tableName, myApp)
	assert.Contains(stmt, "WHERE")
	// SELECT stmt must contain as many placeholders ('$') as there columns in the primary key of myApp
	assert.Equal(2, strings.Count(stmt, "$"))
}

func TestDeleteStmtGeneration(t *testing.T) {
	assert := assert.New(t)
	myApp, _, _ := prepareInput()
	tableName := GetTableName(myApp)

	stmt := getDeleteStmt(tableName, myApp)
	assert.Contains(stmt, "WHERE")
	primaryKey := getPrimaryKey(tableName, myApp)
	// DELETE stmt must contain as many placeholders ('$') as there are columns in the primary key of myApp
	assert.Equal(len(primaryKey), strings.Count(stmt, "$"))
}

func TestInsertStmtGeneration(t *testing.T) {
	assert := assert.New(t)
	myApp, _, _ := prepareInput()
	tableName := GetTableName(myApp)

	stmt, args := getInsertStmt(tableName, myApp)
	assert.Equal(len(args), strings.Count(stmt, "$"))

	assert.Contains(stmt, "VALUES")
	assert.Equal(3, len(args))
	// INSERT stmt must contain as many placeholders ('$') as there are columns in myApp
	assert.Equal(3, strings.Count(stmt, "$"))
}

func TestUpdateStmtGeneration(t *testing.T) {
	assert := assert.New(t)
	myApp, _, _ := prepareInput()
	tableName := GetTableName(myApp)

	stmt, args := getUpdateStmt(tableName, myApp)
	assert.Equal(len(args), strings.Count(stmt, "$"))

	assert.Contains(stmt, "WHERE")
	assert.Equal(3, len(args))
	// UPDATE stmt must contain as many placeholders ('$') in the SET clause as there are non-primary key columns
	setClause := stmt[strings.Index(stmt, "SET"):strings.Index(stmt, "WHERE")]
	assert.Equal(1, strings.Count(setClause, "$"))

	// UPDATE stmt must contain as many placeholders ('$') in the WHERE clause as there are columns in the primary key of myApp
	whereClause := stmt[strings.Index(stmt, "WHERE"):]
	assert.Equal(2, strings.Count(whereClause, "$"))
}

func TestUpsertStmtGeneration(t *testing.T) {
	assert := assert.New(t)
	myApp, _, _ := prepareInput()
	tableName := GetTableName(myApp)

	stmt, args := getUpsertStmt(tableName, myApp)
	assert.Equal(len(args), strings.Count(stmt, "$"))

	onConflictClause := stmt[strings.Index(stmt, "ON CONFLICT"):strings.Index(stmt, "DO UPDATE SET")]
	onConflictClause = strings.TrimSpace(onConflictClause)
	columnNames := onConflictClause[len("ON CONFLICT ("):]
	columnNames = strings.TrimRight(columnNames, ")")
	split := strings.Split(columnNames, ",")
	primaryKey := getPrimaryKey(GetTableName(myApp), myApp)
	assert.Len(split, len(primaryKey), "Expected ON CONFLICT clause to contain only primary key columns")

	valuesClause := stmt[strings.Index(stmt, "VALUES"):strings.Index(stmt, "ON CONFLICT")]
	assert.Equal(3, strings.Count(valuesClause, "$"),
		"Expected the number of dollar signs in VALUES clause to be equal to the number of primary columns")

	updateClause := stmt[strings.Index(stmt, "ON CONFLICT"):]
	assert.Equal(1, strings.Count(updateClause, "$"),
		"Expected the number of dollar signs in UPDATE clause to be equal to the number of non-primary columns")
}

/*
Tests whether all the functions generating SQL statements can accept both structs and pointers to structs.
*/
func TestPassingPointersToStructs(t *testing.T) {
	assert := assert.New(t)
	myApp, user1, _ := prepareInput()
	tableName := GetTableName(myApp)
	tableName2 := GetTableName(user1)

	assert.NotPanics(func() { getSelectStmt(tableName, myApp) })
	assert.NotPanics(func() { getSelectSingleStmt(tableName, myApp) })
	assert.NotPanics(func() { getSelectJoinStmt(tableName, tableName2, myApp, user1, "app_id", 1) })
	assert.NotPanics(func() { getDeleteStmt(tableName, myApp) })
	assert.NotPanics(func() { getInsertStmt(tableName, myApp) })
	assert.NotPanics(func() { getUpdateStmt(tableName, myApp) })

	assert.NotPanics(func() { getSelectStmt(tableName, &myApp) })
	assert.NotPanics(func() { getSelectSingleStmt(tableName, &myApp) })
	assert.NotPanics(func() { getSelectJoinStmt(tableName, tableName2, &myApp, &user1, "app_id", 1) })
	assert.NotPanics(func() { getDeleteStmt(tableName, &myApp) })
	assert.NotPanics(func() { getInsertStmt(tableName, &myApp) })
	assert.NotPanics(func() { getUpdateStmt(tableName, &myApp) })
}

/*
Tests whether primary key is correct stored in an in-memory cache.
*/
func TestGetPrimaryKey(t *testing.T) {
	assert := assert.New(t)
	primaryKeyFields := getPrimaryKey(GetTableName(app{}), app{})
	assert.Equal([]string{"app_id", "org_id"}, primaryKeyFields)

	primaryKeyFields = getPrimaryKey(GetTableName(appUser{}), appUser{})
	assert.Equal([]string{"app_user_id"}, primaryKeyFields)
}

/*
Checks if table name can be extracted from struct/slice of structs using utility methods.
*/
func TestGettingTableName(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("\"appUser\"", GetTableName(appUser{}))
	assert.Equal("\"appUser\"", GetTableName(&appUser{}))

	// Reserved keyword
	assert.Equal("\"group\"", GetTableName(group{}))

	assert.Equal("\"appUser\"", GetTableNameFromSlice([]appUser{}))
	assert.Equal("\"appUser\"", GetTableNameFromSlice(&[]appUser{}))
	assert.Equal("\"appUser\"", GetTableNameFromSlice([]*appUser{}))
	assert.Equal("\"appUser\"", GetTableNameFromSlice(&[]*appUser{}))
}

func TestReplacingPlaceholders(t *testing.T) {
	assert := assert.New(t)

	sqlStmt := "SELECT * FROM my_table WHERE field1 = $1 AND field2 = $2 AND field3 = $3 " +
		"AND field4 = $4 AND field5 = $5 AND field6 = $6 AND field7 = $7 AND field8 = $8 AND field9 = $9 AND field10 = $10"
	sqlStmtWithNoArgs := replacePlaceholdersInSqlStmt(sqlStmt, 1, 2, 3)
	assert.Equal(10, strings.Count(sqlStmtWithNoArgs, "$"),
		"Expected no placeholders to be replaced due to insufficient values to replace those placeholders")

	sqlStmtWithArgs := replacePlaceholdersInSqlStmt(sqlStmt, "xxx", true, false, int8(1), int16(2), int32(3), int64(4), 5, 6, 7)
	assert.Equal(0, strings.Count(sqlStmtWithArgs, "$"), "Expected all placeholders to be replaced by real values")
}
