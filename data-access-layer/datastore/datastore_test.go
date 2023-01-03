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
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Organizations
const COKE = "Coke"
const PEPSI = "Pepsi"

// Service roles for test cases
const TENANT_AUDITOR = "tenant_auditor"
const TENANT_ADMIN = "tenant_admin"
const SERVICE_AUDITOR = "service_auditor"
const SERVICE_ADMIN = "service_admin"
const RANDOM_ID = "SomeRandomId"

var serviceAdminCtx = DataStore.GetAuthorizer().GetAuthContext("", SERVICE_ADMIN)
var cokeAdminCtx = DataStore.GetAuthorizer().GetAuthContext(COKE, TENANT_ADMIN)
var cokeAuditorCtx = DataStore.GetAuthorizer().GetAuthContext(COKE, TENANT_AUDITOR)
var pepsiAdminCtx = DataStore.GetAuthorizer().GetAuthContext(PEPSI, TENANT_ADMIN)
var pepsiAuditorCtx = DataStore.GetAuthorizer().GetAuthContext(PEPSI, TENANT_AUDITOR)

var allResources = []Record{
	app{},
	appUser{},
	appUserInvalid6{},
	appUserInvalid5{},
	appUserInvalid4{},
	appUserInvalid3{},
	appUserInvalid2{},
	appUserInvalid{},
	group{},
}

var datastoreDbTableNames = []string{
	GetTableName(app{}),
	GetTableName(appUser{}),
	GetTableName(appUserInvalid6{}),
	GetTableName(appUserInvalid5{}),
	GetTableName(appUserInvalid4{}),
	GetTableName(appUserInvalid3{}),
	GetTableName(appUserInvalid2{}),
	GetTableName(appUserInvalid{}),
	GetTableName(group{}),
}

//TODO - add a test that would show that the DB users are not able to create, drop, or truncate tables

type appUser struct {
	Id             string `db_column:"app_user_id" primary_key:"true"`
	Name           string `db_column:"name"`
	Email          string `db_column:"email"`
	EmailConfirmed bool   `db_column:"email_confirmed"`
	NumFollowing   int32  `db_column:"num_following"`
	NumFollowers   int64  `db_column:"num_followers"`
	AppId          string `db_column:"app_id"`
	Msg            []byte `db_column:"msg"`
}

type appUserSlice []appUser //Needed for sorting
func (a appUserSlice) Len() int {
	return len(a)
}
func (a appUserSlice) Less(x, y int) bool {
	boolToInt := func(input bool) int {
		var output int = 0
		if input {
			output = 1
		}
		return output
	}

	for i := 0; i < len(a[x].GetId()); i++ {
		first, second := a[x].GetId()[i], a[y].GetId()[i]
		if first == second {
			//Two columns of the composite key are equal - proceed to the next column
			continue
		}

		//Two columns of the composite key are not equal - decide which one is smaller
		switch t := first.(type) {
		case int, int32, int64, int8, int16:
			return first.(int64) < second.(int64)
		case uint, uint32, uint64, uint8, uint16:
			return first.(uint64) < second.(uint64)
		case float32:
			return first.(float32) < second.(float32)
		case float64:
			return first.(float64) < second.(float64)
		case bool:
			return boolToInt(first.(bool)) < boolToInt(second.(bool))
		case string:
			return first.(string) < second.(string)
		default:
			logger.Panicf("Unknown data type %v", t)
			panic(first)
		}
	}
	return false
}
func (a appUserSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

type app struct {
	Id    string `db_column:"app_id" primary_key:"true"`
	Name  string `db_column:"name"`
	OrgId string `db_column:"org_id"`
}

func (a appUser) GetId() []interface{} {
	return []interface{}{a.Id}
}

func (a appUser) areNonKeyFieldsEmpty() bool {
	a.Id = ""
	return cmp.Equal(a, appUser{})
}

func (a app) GetId() []interface{} {
	return []interface{}{a.Id, a.OrgId}
}

func (a app) areNonKeyFieldsEmpty() bool {
	a.Id = ""
	a.OrgId = ""
	return cmp.Equal(a, app{})
}

// Input for all test cases. Inserted into data store by prepareInput()
var myCokeApp app
var user1 appUser
var user2 appUser

/*
Prepared input for all test cases. Stores the input in global variables.
*/
func prepareInput() (app, appUser, appUser) {
	rand.Seed(time.Now().Unix())
	myCokeApp = app{
		Id:    "id-" + strconv.Itoa(rand.Int()),
		Name:  "Cool_app",
		OrgId: COKE,
	}

	user1 = appUser{
		Id:             "id-" + strconv.Itoa(rand.Int()),
		Name:           "Jeyhun",
		Email:          "jeyhun@mail.com",
		EmailConfirmed: true,
		NumFollowing:   2147483647,          //int32 type
		NumFollowers:   9223372036854775807, //int64 type
		AppId:          myCokeApp.Id,
		Msg:            []byte("msg1234"),
	}

	user2 = appUser{
		Id:             "id-" + strconv.Itoa(rand.Int()),
		Name:           "Jahangir",
		Email:          "jahangir@mail.com",
		EmailConfirmed: false,
		NumFollowing:   2,
		NumFollowers:   20,
		AppId:          myCokeApp.Id,
		Msg:            []byte("msg9876"),
	}

	//Make sure the 2 users  are sorted in ascending order by ID
	if user1.Id >= user2.Id {
		tempUser := user1
		user1 = user2
		user2 = tempUser
	}

	return myCokeApp, user1, user2

}

func createDbTables(ctx context.Context) error {
	roleMapping := map[string]DbRole{
		TENANT_AUDITOR:  TENANT_READER,
		TENANT_ADMIN:    TENANT_WRITER,
		SERVICE_AUDITOR: READER,
		SERVICE_ADMIN:   WRITER,
	}
	for _, record := range []Record{app{}, appUser{}} {
		if err := DataStore.RegisterWithDAL(ctx, roleMapping, record); err != nil {
			return err
		}
	}
	return nil
}

func TestTruncate(t *testing.T) {
	assert := assert.New(t)

	setupDbTables(t)

	var queryResults = make([]app, 0)
	if err := DataStore.FindAll(cokeAdminCtx, &queryResults); err != nil {
		assert.FailNow(fmt.Sprintf("Failed to query DB table %s: %s", GetTableName(app{}), err.Error()))
	} else if len(queryResults) == 0 {
		assert.FailNow("Failed to set up test case")
	}

	err := TestHelper.Truncate(GetTableName(app{}))
	assert.NoError(err)

	queryResults = make([]app, 0)
	err = DataStore.FindAll(cokeAdminCtx, &queryResults)
	assert.NoError(err)
	assert.Empty(queryResults, "Expected all records to be deleted after table truncate")
}

func TestTruncateNonExistent(t *testing.T) {
	assert := assert.New(t)
	err := TestHelper.Truncate("non_existent_table")
	assert.NoError(err, "Expected no error when trying to truncate a non-existent table")
}

func testFindInEmptyDataStore(t *testing.T) {
	assert := assert.New(t)

	{
		queryResult := appUser{Id: "non-existent user ID"}
		err := DataStore.Find(cokeAdminCtx, &queryResult)
		assert.ErrorIs(err, RecordNotFoundError)
		assert.True(queryResult.areNonKeyFieldsEmpty())
	}

	{
		records := make([]appUser, 0)
		err := DataStore.FindAll(cokeAdminCtx, &records)
		assert.NoError(err)
		assert.Empty(records)
	}

	{
		records := make([]app, 0)
		err := DataStore.FindAll(cokeAdminCtx, &records)
		assert.NoError(err)
		assert.Empty(records)
	}
}

func TestFindInEmptyDatabase(t *testing.T) {
	setupEmptyDbTables(t)
	testFindInEmptyDataStore(t)
}

func TestFindInEmptyInMemory(t *testing.T) {
	setupEmptyInMemoryCache(t)
	testFindInEmptyDataStore(t)
}

func testCrud(t *testing.T, ctx context.Context) {
	assert := assert.New(t)

	var err error

	//Querying of previously inserted records should succeed
	for _, record := range []appUser{user1, user2} {
		var queryResult appUser = appUser{Id: record.Id}
		err = DataStore.Find(ctx, &queryResult)
		assert.NoError(err)
		assert.Equal(record, queryResult)
	}

	//Updating non-key fields in a record should succeed
	user1.Name = "Jeyhun G."
	user1.Email = "jeyhun111@mail.com"
	user1.EmailConfirmed = !user1.EmailConfirmed
	user1.NumFollowers++
	user2.Name = "Jahangir G."
	user2.Email = "jahangir111@mail.com"
	user2.EmailConfirmed = !user2.EmailConfirmed
	user2.NumFollowers--
	for _, record := range []appUser{user1, user2} {
		rowsAffected, err := DataStore.Update(ctx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)
		var queryResult appUser = appUser{Id: record.Id}
		err = DataStore.Find(ctx, &queryResult)
		assert.NoError(err)
		assert.Equal(record, queryResult)
	}

	//Upsert operation should be an update for already existing records
	user1.NumFollowers++
	user2.NumFollowers--
	for _, record := range []appUser{user1, user2} {
		rowsAffected, err := DataStore.Upsert(ctx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)
		var queryResult appUser = appUser{Id: record.Id}
		err = DataStore.Find(ctx, &queryResult)
		assert.NoError(err)
		assert.Equal(record, queryResult)
	}

	//Deletion of existing records should not fail, and the records should no longer be found in the DB
	for _, record := range []appUser{user1, user2} {
		rowsAffected, err := DataStore.Delete(ctx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)
		queryResult := appUser{Id: record.Id}
		err = DataStore.Find(ctx, &queryResult)
		assert.ErrorIs(err, RecordNotFoundError)
		assert.True(queryResult.areNonKeyFieldsEmpty())
	}
}

/*
Drops existing tables and creates new ones. Generates a context object with a specific org. and CSP role.
*/
func setupEmptyDbTables(t *testing.T) {
	assert := assert.New(t)
	DataStore.Configure(serviceAdminCtx, false, DataStore.GetAuthorizer())
	allTableNames := make([]string, 0, len(datastoreDbTableNames))
	allTableNames = append(allTableNames, datastoreDbTableNames...)
	if err := TestHelper.Drop(allTableNames...); err != nil {
		assert.FailNow("Failed to drop DB tables for the following reason:\n" + err.Error())
	}
	if err := createDbTables(serviceAdminCtx); err != nil {
		assert.FailNow("Failed to create DB tables for the following reason:\n" + err.Error())
	}
}

/*
Drops existing tables and creates new ones. Generates a context object with a specific org. and role.
Adds the records returned by prepareInput() to the DB tables.
*/
func setupDbTables(t *testing.T) {
	setupEmptyDbTables(t)
	myCokeApp, user1, user2 := prepareInput()
	for _, record := range []Record{myCokeApp, user1, user2} {
		if _, err := DataStore.Insert(cokeAdminCtx, record); err != nil {
			assert.FailNow(t, "Failed to prepare data for test case:\n"+err.Error())
		}
	}
}

func setupEmptyInMemoryCache(t *testing.T) {
	DataStore.Configure(cokeAdminCtx, true, DataStore.GetAuthorizer())
	DataStore.Reset()
}

func setupInMemoryCache(t *testing.T) {
	t.Helper()
	setupEmptyInMemoryCache(t)
	myCokeApp, user1, user2 := prepareInput()
	for _, record := range []Record{myCokeApp, user1, user2} {
		if _, err := DataStore.Insert(cokeAdminCtx, record); err != nil {
			assert.FailNow(t, "Failed to prepare data for test case")
		}
	}
}

func TestMain(m *testing.M) {
	ClearAllTables()
	code := m.Run()
	ClearAllTables()
	os.Exit(code)
}

func BenchmarkCrudDatabase(b *testing.B) {
	rawLogger := logrus.New()
	rawLogger.SetLevel(logrus.FatalLevel)
	rawLogger.SetOutput(io.Discard)
	logger = rawLogger.WithField("comp", "saas-persistence")

	var t testing.T
	setupDbTables(&t)
	for n := 0; n < b.N; n++ {
		testCrud(&t, cokeAdminCtx)
	}
}

func BenchmarkCrudInMemory(b *testing.B) {
	rawLogger := logrus.New()
	rawLogger.SetLevel(logrus.FatalLevel)
	rawLogger.SetOutput(io.Discard)
	logger = rawLogger.WithField("comp", "saas-persistence")

	var t testing.T
	setupInMemoryCache(&t)
	for n := 0; n < b.N; n++ {
		testCrud(&t, cokeAdminCtx)
	}
}

func TestCrudDatabase(t *testing.T) {
	setupDbTables(t)
	testCrud(t, cokeAdminCtx)
}

func TestCrudInMemory(t *testing.T) {
	setupInMemoryCache(t)
	testCrud(t, cokeAdminCtx)
}

func testFindAll(t *testing.T, ctx context.Context) {
	assert := assert.New(t)

	//FindAll should return all (two) records
	queryResults := make([]appUser, 0)
	err := DataStore.FindAll(ctx, &queryResults)
	sort.Sort(appUserSlice(queryResults))
	assert.NoError(err)
	assert.Len(queryResults, 2)

	expected := []appUser{user1, user2}

	for i := 0; i < len(queryResults); i++ {
		assert.Equal(expected[i], queryResults[i])
	}
}

func testFindWithCriteria(t *testing.T, ctx context.Context) {
	assert := assert.New(t)

	expected := []appUser{user1, user2}

	//Pass filtering criteria
	for _, user := range expected {
		//Search by all fields
		queryResults := make([]appUser, 0)
		err := DataStore.FindWithFilter(ctx, &user, &queryResults)
		assert.NoError(err)
		assert.Len(queryResults, 1)
		assert.Equal(user, queryResults[0])

		//Search only by name
		queryResults = make([]appUser, 0)
		err = DataStore.FindWithFilter(ctx, &appUser{Name: user.Name}, &queryResults)
		assert.NoError(err)
		assert.Len(queryResults, 1)
		assert.Equal(user, queryResults[0])
	}
}

func TestFindAllDatabase(t *testing.T) {
	setupDbTables(t)
	testFindAll(t, cokeAdminCtx)
}

func TestFindAllInMemory(t *testing.T) {
	setupInMemoryCache(t)
	testFindAll(t, cokeAdminCtx)
}

func TestFindWithCriteriaDatabase(t *testing.T) {
	setupDbTables(t)
	testFindWithCriteria(t, cokeAdminCtx)
}

func TestCrudWithMissingOrgId(t *testing.T) {
	assert := assert.New(t)
	setupEmptyDbTables(t)
	_, apps := make([]appUser, 0), make([]app, 0)

	//Insert some data, to make sure that DAL methods fail not due to data missing in data store
	rowsAffected, err := DataStore.Insert(cokeAdminCtx, appUser{Id: RANDOM_ID})
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)

	//FIND ALL
	err = DataStore.FindAll(context.Background(), &apps) //Missing org. ID in context
	assert.ErrorIs(err, ErrorFetchingMetadataFromContext)

	//FIND BY ID
	err = DataStore.Find(context.Background(), &app{Id: "random ID"}) //Missing org. ID in context
	assert.ErrorIs(err, ErrorFetchingMetadataFromContext)

	//DELETE
	_, err = DataStore.Delete(context.Background(), app{Id: "random ID"}) //Missing org. ID in context
	assert.ErrorIs(err, ErrorFetchingMetadataFromContext)

	//INSERT
	rowsAffected, err = DataStore.Insert(context.Background(), app{Id: RANDOM_ID}) //Missing org. ID in context
	assert.ErrorIs(err, ErrorFetchingMetadataFromContext)
	assert.Equal(int64(0), rowsAffected)

	//UPDATE
	_, err = DataStore.Update(context.Background(), app{Id: RANDOM_ID}) //Missing org. ID in context
	assert.ErrorIs(err, ErrorFetchingMetadataFromContext)

	_, err = DataStore.Delete(cokeAdminCtx, appUser{Id: RANDOM_ID})
	assert.NoError(err)

	//JOIN ONE TO MANY
	var queryResult2 appUserSlice = make([]appUser, 1)
	err = DataStore.PerformJoinOneToMany(context.Background(), &app{}, RANDOM_ID, "app_id", &queryResult2) //Missing org. ID
	assert.ErrorIs(err, ErrorFetchingMetadataFromContext)

	err = DataStore.PerformJoinOneToOne(context.Background(), &app{}, RANDOM_ID, &appUser{}, "app_id") //Missing org. ID
	assert.ErrorIs(err, ErrorFetchingMetadataFromContext)
}

func testCrudWithInvalidParams(t *testing.T, ctx context.Context) {
	assert := assert.New(t)

	//Insert some data, to make sure that DAL methods fail not due to data missing in data store
	rowsAffected, err := DataStore.Insert(ctx, appUser{Id: RANDOM_ID})
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)

	//FIND ALL
	var apps []app
	err = DataStore.FindAll(ctx, apps) //Passing a nil slice
	assert.ErrorIs(err, IllegalArgumentError)

	apps = make([]app, 0)

	//FIND ALL
	err = DataStore.FindAll(ctx, apps) //Passing slice by value
	assert.ErrorIs(err, IllegalArgumentError)

	//FIND ALL
	err = DataStore.FindAll(ctx, &app{}) //Passing a struct by reference (instead of a slice of structs by reference)
	assert.ErrorIs(err, IllegalArgumentError)

	//FIND BY ID
	err = DataStore.Find(ctx, appUser{}) //Passing a struct instead of a pointer to a struct
	assert.ErrorIs(err, IllegalArgumentError)

	//JOIN ONE TO MANY
	var queryResult2 appUserSlice = make([]appUser, 1)
	err = DataStore.PerformJoinOneToMany(ctx, app{}, RANDOM_ID, "app_id", &queryResult2) //Passing a struct instead of a pointer to a struct
	assert.ErrorIs(err, IllegalArgumentError)

	err = DataStore.PerformJoinOneToMany(ctx, &app{}, "", "app_id", &queryResult2) //Passing an empty ID
	assert.ErrorIs(err, IllegalArgumentError)

	err = DataStore.PerformJoinOneToMany(ctx, &app{}, "    ", "app_id", &queryResult2) //Passing a blank ID
	assert.ErrorIs(err, IllegalArgumentError)

	err = DataStore.PerformJoinOneToMany(ctx, &app{}, RANDOM_ID, "app_id", make([]appUser, 1)) //Passing a slice instead of a pointer to a slice
	assert.ErrorIs(err, IllegalArgumentError)

	err = DataStore.PerformJoinOneToMany(ctx, &app{}, RANDOM_ID, "app_id", &appUser{}) //Passing a pointer but not to a slice
	assert.ErrorIs(err, IllegalArgumentError)

	err = DataStore.PerformJoinOneToMany(ctx, &app{}, RANDOM_ID, "", &queryResult2) //Passing an empty join column name
	assert.ErrorIs(err, IllegalArgumentError)

	err = DataStore.PerformJoinOneToMany(ctx, &app{}, RANDOM_ID, "    ", &queryResult2) //Passing a blank join column name
	assert.ErrorIs(err, IllegalArgumentError)

	queryResult2 = make([]appUser, 0)
	err = DataStore.PerformJoinOneToMany(ctx, &app{}, RANDOM_ID, "app_id", &queryResult2) //Passing an empty slice
	assert.ErrorIs(err, IllegalArgumentError)

	//JOIN ONE TO ONE
	err = DataStore.PerformJoinOneToOne(ctx, app{}, RANDOM_ID, &appUser{}, "app_id") //Passing a struct instead of a pointer to a struct
	assert.ErrorIs(err, IllegalArgumentError)

	err = DataStore.PerformJoinOneToOne(ctx, &app{}, "", &appUser{}, "app_id") //Passing an empty ID
	assert.ErrorIs(err, IllegalArgumentError)

	err = DataStore.PerformJoinOneToOne(ctx, &app{}, "    ", &appUser{}, "app_id") //Passing a blank ID
	assert.ErrorIs(err, IllegalArgumentError)

	err = DataStore.PerformJoinOneToOne(ctx, &app{}, RANDOM_ID, appUser{}, "app_id") //Passing a struct instead of a pointer to a struct
	assert.ErrorIs(err, IllegalArgumentError)

	err = DataStore.PerformJoinOneToOne(ctx, &app{}, RANDOM_ID, &appUser{}, "") //Passing an empty join column name
	assert.ErrorIs(err, IllegalArgumentError)

	err = DataStore.PerformJoinOneToOne(ctx, &app{}, RANDOM_ID, &appUser{}, "    ") //Passing a blank column name
	assert.ErrorIs(err, IllegalArgumentError)
}

func TestCrudWithInvalidParamsDatabase(t *testing.T) {
	assert := assert.New(t)
	testCrudWithInvalidParams(t, cokeAdminCtx)

	_, err := DataStore.Insert(cokeAuditorCtx, &app{OrgId: COKE, Id: "foo"})
	assert.ErrorIs(err, ErrorExecutingSqlStmt)

	_, err = DataStore.Insert(pepsiAuditorCtx, &appUser{Id: "foo"})
	assert.ErrorIs(err, ErrorExecutingSqlStmt)
}

func TestCrudWithInvalidParamsInMemory(t *testing.T) {
	setupEmptyInMemoryCache(t)
	testCrudWithInvalidParams(t, cokeAdminCtx)
}

func testJoins(t *testing.T, ctx context.Context) {
	assert := assert.New(t)

	{
		//Perform join
		var queryResult1 app = app{}
		var queryResult2 appUserSlice = make([]appUser, 1)
		err := DataStore.PerformJoinOneToMany(ctx, &queryResult1, myCokeApp.GetId()[0].(string), "app_id", &queryResult2)
		sort.Sort(queryResult2)
		assert.NoError(err)

		assert.Equal(myCokeApp, queryResult1)

		//There should be 2 output records
		assert.Len(queryResult2, 2)

		//First and second output record should contain first and second user's info, respectively
		assert.Equal(user1, queryResult2[0])
		assert.Equal(user2, queryResult2[1])
	}

	{
		//Perform join, while making an assumption that the relationship between app and appUser is one-to-one
		var queryResult1 app = app{}
		var queryResult2 appUser = appUser{}
		err := DataStore.PerformJoinOneToOne(ctx, &queryResult1, myCokeApp.GetId()[0].(string), &queryResult2, "app_id")
		assert.NoError(err)

		assert.Equal(myCokeApp, queryResult1)
		assert.True(cmp.Equal(user1, queryResult2) || cmp.Equal(user2, queryResult2))
	}
}

func TestJoinsDatabase(t *testing.T) {
	setupDbTables(t)
	testJoins(t, cokeAdminCtx)
}

func TestJoinsInMemory(t *testing.T) {
	t.SkipNow() //Skip this test case for now - joins in an in-memory cache do not work for now. Would need to change the signature of join methods to make it work
	setupInMemoryCache(t)
	testJoins(t, cokeAdminCtx)
}

// Not all fields contain a tag for column name
type appUserInvalid struct {
	Id             string `db_column:"id" primary_key:"true"`
	Name           string
	Email          string
	EmailConfirmed bool
	NumFollowing   int32
	NumFollowers   int64
	AppId          string
}

func (appUser appUserInvalid) GetId() []interface{} {
	return []interface{}{appUser.Id}
}

// Does not contain a tag for primary key
type appUserInvalid2 struct {
	Id             string `db_column:"id"`
	Name           string `db_column:"name"`
	Email          string `db_column:"email"`
	EmailConfirmed bool   `db_column:"email_confirmed"`
	NumFollowing   int32  `db_column:"num_following"`
	NumFollowers   int64  `db_column:"num_followers"`
	AppId          string `db_column:"app_id"`
}

func (appUser appUserInvalid2) GetId() []interface{} {
	return []interface{}{appUser.Id}
}

// Contains a field with an unsupported data type (unsigned int)
type appUserInvalid3 struct {
	Id             string `db_column:"id" primary_key:"true"`
	Name           string `db_column:"name"`
	Email          string `db_column:"email"`
	EmailConfirmed bool   `db_column:"email_confirmed"`
	NumFollowing   int32  `db_column:"num_following"`
	NumFollowers   uint64 `db_column:"num_followers"`
	AppId          string `db_column:"app_id"`
}

func (appUser appUserInvalid3) GetId() []interface{} {
	return []interface{}{appUser.Id}
}

// Contains a field with an unexported field
type appUserInvalid4 struct {
	Id             string `db_column:"id" primary_key:"true"`
	Name           string `db_column:"name"`
	Email          string `db_column:"email"`
	EmailConfirmed bool   `db_column:"email_confirmed"`
	NumFollowing   int32  `db_column:"num_following"`
	NumFollowers   int64  `db_column:"num_followers"`
	appId          string `db_column:"app_id"`
}

func (appUser appUserInvalid4) GetId() []interface{} {
	return []interface{}{appUser.Id}
}

// Contains a primary_key tag that doesn't have a boolean value
type appUserInvalid5 struct {
	Id             string `db_column:"id" primary_key:"xxx"`
	Name           string `db_column:"name"`
	Email          string `db_column:"email"`
	EmailConfirmed bool   `db_column:"email_confirmed"`
	NumFollowing   int32  `db_column:"num_following"`
	NumFollowers   int64  `db_column:"num_followers"`
	AppId          string `db_column:"app_id"`
}

func (appUser appUserInvalid5) GetId() []interface{} {
	return []interface{}{appUser.Id}
}

// Contains duplicate values for db_column tag
type appUserInvalid6 struct {
	Id             string `db_column:"id" primary_key:"true"`
	Name           string `db_column:"name"`
	Email          string `db_column:"email"`
	EmailConfirmed bool   `db_column:"email_confirmed"`
	NumFollowing   int32  `db_column:"num_following"`
	NumFollowers   int64  `db_column:"num_following"`
	AppId          string `db_column:"app_id"`
}

func (appUser appUserInvalid6) GetId() []interface{} {
	return []interface{}{appUser.Id}
}

type group struct {
	Id       string `db_column:"id" primary_key:"true"`
	Name     string `db_column:"name"`
	Revision int    `db_column:"_revision"`
}

func (group group) GetId() []interface{} {
	return []interface{}{group.Id}
}

func testDALRegistration(t *testing.T, ctx context.Context) {
	assert := assert.New(t)
	roleMapping := map[string]DbRole{SERVICE_AUDITOR: READER}

	//Not all fields contain a tag for column name
	assert.Panics(func() { _ = DataStore.RegisterWithDAL(ctx, roleMapping, appUserInvalid{}) })

	//Does not contain a tag for primary key
	assert.Panics(func() { _ = DataStore.RegisterWithDAL(ctx, roleMapping, appUserInvalid2{}) })

	//Contains a field with an unsupported data type (unsigned int)
	assert.Panics(func() { _ = DataStore.RegisterWithDAL(ctx, roleMapping, appUserInvalid3{}) })

	//Contains a field with an unexported field
	assert.Panics(func() { _ = DataStore.RegisterWithDAL(ctx, roleMapping, appUserInvalid4{}) })
	logger.Debug(appUserInvalid4{}.appId) //Need to use this field to pass lint

	//Contains a primary_key tag that doesn't have a boolean value
	assert.Panics(func() { _ = DataStore.RegisterWithDAL(ctx, roleMapping, appUserInvalid5{}) })

	//Contains duplicate DB column names (duplicate values for TAG_DB_COLUMN tag)
	assert.Panics(func() { _ = DataStore.RegisterWithDAL(ctx, roleMapping, appUserInvalid6{}) })

	//When registering a struct with DAL, you should be able to pass it either by value or by reference
	err := DataStore.RegisterWithDAL(ctx, roleMapping, app{})
	assert.NoError(err)

	err = DataStore.RegisterWithDAL(ctx, roleMapping, &appUser{})
	assert.NoError(err)

	//You should be able to register a struct that happens to have a name that's a reserved keyword in Postgres
	err = DataStore.RegisterWithDAL(ctx, roleMapping, &group{})
	assert.NoError(err)
}

func TestDALRegistrationDatabase(t *testing.T) {
	setupEmptyDbTables(t)
	testDALRegistration(t, cokeAdminCtx)

	assert := assert.New(t)
	//Remove DB connections. Check if RegisterWithDAL() is still able to reconnect to DB
	relationalDb.dbMap = make(map[DbRole]*sql.DB)
	err := DataStore.RegisterWithDAL(cokeAdminCtx, map[string]DbRole{SERVICE_AUDITOR: READER}, &appUser{})
	assert.NoError(err)

}

func TestDALRegistrationInMemory(t *testing.T) {
	setupEmptyInMemoryCache(t)
	testDALRegistration(t, cokeAdminCtx)
}

/*
Checks if DAL is able to select the least restrictive available DB role to perform SQL operations on one table.
*/
func TestDeleteWithMultipleCSPRoles(t *testing.T) {
	const APP_ADMIN = "app_admin"
	assert := assert.New(t)

	//Create context for custom admin who will have 2 service roles
	customCtx := DataStore.GetAuthorizer().GetAuthContext(COKE, APP_ADMIN, SERVICE_ADMIN)
	DataStore.Configure(customCtx, false, DataStore.GetAuthorizer())

	if err := TestHelper.Drop(datastoreDbTableNames...); err != nil {
		assert.FailNow("Failed to drop DB tables for the following reason:\n" + err.Error())
	}

	//The custom admin will have a read access being an app admin and read & write access being a service admin
	roleMapping := map[string]DbRole{APP_ADMIN: READER, SERVICE_ADMIN: WRITER}
	err := DataStore.RegisterWithDAL(customCtx, roleMapping, app{})
	if err != nil {
		assert.FailNow("Failed to setup the test case for the following reason:\n")
	}

	//Add some data to the app table
	prepareInput()
	if _, err = DataStore.Insert(cokeAdminCtx, myCokeApp); err != nil {
		assert.FailNow("Failed to prepare data for the test case for the following reason:\n" + err.Error())
	}

	//Make sure that the custom admin is able to delete the data
	rowsAffected, err := DataStore.Delete(customCtx, myCokeApp)
	assert.NoError(err)
	assert.EqualValues(1, rowsAffected)
}

/*
Tries to perform a query with a service role that has not been authorized to access the table
*/
func TestUnauthorizedAccess(t *testing.T) {
	assert := assert.New(t)
	setupDbTables(t)

	ctx := DataStore.GetAuthorizer().GetAuthContext(COKE, "unauthorized service role")
	var queryResult = app{Id: myCokeApp.Id, OrgId: PEPSI}
	err := DataStore.Find(ctx, &queryResult)
	assert.ErrorIs(err, ErrOperationNotAllowed)
}

/*
Tries CRUD operations with Pepsi's org ID in the context, while the data in DB belongs to Coke
*/
func testCrudWithMismatchingOrgId(t *testing.T, cokeCtx context.Context) {
	assert := assert.New(t)
	tenantStr := "tenant=Pepsi"
	orgIdStr := "orgIdCol=Coke"

	//Try to perform CRUD operations on Coke's data as a user from Pepsi
	{
		var queryResult1 app = app{}
		var queryResult2 appUserSlice = make([]appUser, 1)
		err := DataStore.PerformJoinOneToMany(pepsiAdminCtx, &queryResult1, myCokeApp.GetId()[0].(string), "app_id", &queryResult2)
		assert.NoError(err)
		assert.True(queryResult1.areNonKeyFieldsEmpty())
		assert.Empty(queryResult2)
	}

	{
		var queryResult1 app = app{}
		var queryResult2 appUser = appUser{}
		err := DataStore.PerformJoinOneToOne(pepsiAdminCtx, &queryResult1, myCokeApp.GetId()[0].(string), &queryResult2, "app_id")
		assert.NoError(err)
		assert.True(queryResult1.areNonKeyFieldsEmpty())
		assert.True(queryResult2.areNonKeyFieldsEmpty())
	}

	{
		var queryResult app = app{Id: myCokeApp.Id, OrgId: myCokeApp.OrgId}
		err := DataStore.Find(pepsiAdminCtx, &queryResult)
		assert.ErrorIs(err, ErrOperationNotAllowed) //Trying to read another tenant's data should return an error
		assert.True(strings.Contains(err.Error(), tenantStr), err.Error())
		assert.True(strings.Contains(err.Error(), orgIdStr), err.Error())
	}

	{
		queryResult := make([]app, 0)
		err := DataStore.FindAll(pepsiAdminCtx, &queryResult)
		assert.NoError(err)       //Pepsi tried to read all the records in DB - no error should be returned
		assert.Empty(queryResult) //But Pepsi should not see Coke's data

		//Try to read a specific record from DB that definitely exists but belongs to another tenant
		queryResult = make([]app, 0)
		err = DataStore.FindWithFilter(pepsiAdminCtx, &myCokeApp, &queryResult)
		assert.ErrorIs(err, ErrOperationNotAllowed)
		assert.True(strings.Contains(err.Error(), tenantStr))
		assert.True(strings.Contains(err.Error(), orgIdStr))
	}

	{
		_, err := DataStore.Delete(pepsiAdminCtx, myCokeApp)
		assert.ErrorIs(err, ErrOperationNotAllowed) //Trying to delete another tenant's data should return an error
		queryResult := app{Id: myCokeApp.Id, OrgId: myCokeApp.OrgId}
		err = DataStore.Find(cokeCtx, &queryResult) //Since the previous delete has failed, the data should still be in the DB
		assert.NoError(err)
		assert.NotEmpty(queryResult)
	}

	{
		newApp := app{
			Id:    "id-" + strconv.Itoa(rand.Int()),
			Name:  "New app",
			OrgId: COKE,
		}

		//You should not be able to insert another tenant's data into data store
		rowsAffected, err := DataStore.Insert(pepsiAdminCtx, newApp)
		assert.ErrorIs(err, ErrOperationNotAllowed)
		assert.Equal(int64(0), rowsAffected)

		//Another tenant's data should not be found because it should not have been inserted into the data store
		queryResult := app{Id: newApp.Id, OrgId: newApp.OrgId}
		err = DataStore.Find(cokeCtx, &queryResult)
		assert.ErrorIs(err, RecordNotFoundError)
		assert.True(queryResult.areNonKeyFieldsEmpty())
	}

	{
		//App without an org. ID
		newApp := app{
			Id:   "id-" + strconv.Itoa(rand.Int()),
			Name: "New app",
		}

		//You should not be able to insert a "multi-tenant" record that lacks the org. ID
		rowsAffected, err := DataStore.Insert(pepsiAdminCtx, newApp)
		assert.ErrorIs(err, ErrorExecutingSqlStmt)
		assert.Equal(int64(0), rowsAffected)
	}

	{
		updatedApp := myCokeApp
		updatedApp.Name = "new name"
		_, err := DataStore.Update(pepsiAdminCtx, updatedApp) //You shouldn't be able to update another tenant's data with a tenant-specific role
		assert.ErrorIs(err, ErrOperationNotAllowed)

		queryResult := app{Id: myCokeApp.Id, OrgId: myCokeApp.OrgId}
		err = DataStore.Find(cokeCtx, &queryResult)
		assert.NoError(err)
		assert.NotEqual(updatedApp.Name, queryResult.Name) //Record should not have been updated
		assert.Equal(myCokeApp.Name, queryResult.Name)     //Record should not have been updated
	}
}

func TestCrudWithMismatchingOrgIdDatabase(t *testing.T) {
	setupDbTables(t)
	testCrudWithMismatchingOrgId(t, cokeAdminCtx)
}

func ClearAllTables() {
	_ = TestHelper.DropTables(allResources...)
}

func TestWithMissingEnvVar(t *testing.T) {
	assert := assert.New(t)

	for _, envVar := range []string{DB_ADMIN_USERNAME_ENV_VAR, DB_ADMIN_PASSWORD_ENV_VAR, DB_NAME_ENV_VAR, DB_PORT_ENV_VAR, DB_HOST_ENV_VAR, SSL_MODE_ENV_VAR} {
		defer os.Setenv(envVar, os.Getenv(envVar))
		os.Unsetenv(envVar)
	}

	assert.ErrorIs(relationalDb.initialize(), ErrorMissingEnvVar)
}

func TestWithEmptyEnvVar(t *testing.T) {
	assert := assert.New(t)

	for _, envVar := range []string{DB_ADMIN_USERNAME_ENV_VAR, DB_ADMIN_PASSWORD_ENV_VAR, DB_NAME_ENV_VAR, DB_PORT_ENV_VAR, DB_HOST_ENV_VAR, SSL_MODE_ENV_VAR} {
		defer os.Setenv(envVar, os.Getenv(envVar))
		os.Setenv(envVar, "")
	}

	assert.ErrorIs(relationalDb.initialize(), ErrorMissingEnvVar)
}

func TestWithBlankEnvVar(t *testing.T) {
	assert := assert.New(t)

	for _, envVar := range []string{DB_ADMIN_USERNAME_ENV_VAR, DB_ADMIN_PASSWORD_ENV_VAR, DB_NAME_ENV_VAR, DB_PORT_ENV_VAR, DB_HOST_ENV_VAR, SSL_MODE_ENV_VAR} {
		defer os.Setenv(envVar, os.Getenv(envVar))
		os.Setenv(envVar, "    ")
	}

	assert.ErrorIs(relationalDb.initialize(), ErrorMissingEnvVar)
}

/*
Checks that you can correctly sort DB roles based how restrictive they are (most restrictive to the least restrictive roles).
E.g., tenant_reader more restrictive than reader
*/
func TestDbRoleSorting(t *testing.T) {
	assert := assert.New(t)

	var roles DbRoleSlice = []DbRole{WRITER, TENANT_WRITER, READER, TENANT_READER}
	var writerIndex, tenantWriterIndex, readerIndex, tenantReaderIndex = 0, 1, 2, 3
	assert.False(roles.Less(writerIndex, tenantWriterIndex))
	assert.False(roles.Less(tenantWriterIndex, readerIndex))
	assert.False(roles.Less(readerIndex, tenantReaderIndex))
	assert.False(roles.Less(writerIndex, readerIndex))

	sort.Sort(roles)
	assert.True(roles.Less(0, 3))
}

/*
Tests revision blocking updates that are outdated
*/
func TestRevision(t *testing.T) {
	assert := assert.New(t)

	DataStore.Configure(cokeAdminCtx, false, DataStore.GetAuthorizer())
	if err := TestHelper.Drop(GetTableName(group{})); err != nil {
		assert.FailNow("Failed to drop DB tables for the following reason:\n" + err.Error())
	}
	roleMapping := map[string]DbRole{
		TENANT_AUDITOR:  TENANT_READER,
		TENANT_ADMIN:    TENANT_WRITER,
		SERVICE_AUDITOR: READER,
		SERVICE_ADMIN:   WRITER,
	}

	err := DataStore.RegisterWithDAL(cokeAdminCtx, roleMapping, group{})
	assert.NoError(err)
	var myGroup = group{Id: "withRevisionId-12345", Name: "withRevisionName"}

	//FIXME Initial revision should be 1
	const initialRevision int = 0

	_, err = DataStore.Insert(cokeAdminCtx, myGroup)
	assert.NoError(err)

	var actualQueryResult, expectedQueryResult = group{Id: myGroup.Id}, group{Id: "withRevisionId-12345", Name: "withRevisionName", Revision: initialRevision}
	err = DataStore.Find(cokeAdminCtx, &actualQueryResult)
	assert.NoError(err)
	assert.Equal(expectedQueryResult, actualQueryResult)

	//update should  succeed when revision is the same as the value in the db
	updatedGroup := group{Id: myGroup.Id, Name: "updatedGroup", Revision: initialRevision}
	rowsAffected, err := DataStore.Update(cokeAdminCtx, updatedGroup)
	assert.NoError(err)
	assert.EqualValues(1, rowsAffected)
	actualQueryResult = group{Id: myGroup.Id}
	err = DataStore.Find(cokeAdminCtx, &actualQueryResult)
	assert.NoError(err)
	assert.Equal(initialRevision+1, actualQueryResult.Revision, "Expected revision to be incremented by 1 after update")
	assert.Equal(updatedGroup.Name, actualQueryResult.Name)

	//upsert should  succeed when revision is the same as the value in the db
	updatedGroup = group{Id: myGroup.Id, Name: "updatedGroup2", Revision: actualQueryResult.Revision}
	rowsAffected, err = DataStore.Upsert(cokeAdminCtx, updatedGroup)
	assert.NoError(err)
	assert.EqualValues(1, rowsAffected)
	actualQueryResult = group{Id: myGroup.Id}
	err = DataStore.Find(cokeAdminCtx, &actualQueryResult)
	assert.NoError(err)
	assert.Equal(initialRevision+2, actualQueryResult.Revision, "Expected revision to be incremented by 1 after upsert")
	assert.Equal(updatedGroup.Name, actualQueryResult.Name)

	//update should fail when revision is not equal to the value in the db
	updatedGroup = group{Id: myGroup.Id, Name: "updatedGroup3", Revision: initialRevision + 100}
	_, err = DataStore.Update(cokeAdminCtx, updatedGroup)
	assert.ErrorIs(err, RevisionConflictError)
	actualQueryResult = group{Id: myGroup.Id}
	err = DataStore.Find(cokeAdminCtx, &actualQueryResult)
	assert.NoError(err)
	assert.NotEqual(updatedGroup.Revision+1, actualQueryResult.Revision)
	assert.NotEqual(updatedGroup.Name, actualQueryResult.Name)

	//upsert should fail when revision is not equal to the value in the db
	updatedGroup = group{Id: myGroup.Id, Name: "updatedGroup4", Revision: initialRevision + 100}
	_, err = DataStore.Upsert(cokeAdminCtx, updatedGroup)
	assert.ErrorIs(err, RevisionConflictError)
	actualQueryResult = group{Id: myGroup.Id}
	err = DataStore.Find(cokeAdminCtx, &actualQueryResult)
	assert.NoError(err)
	assert.NotEqual(updatedGroup.Revision+1, actualQueryResult.Revision)
	assert.NotEqual(updatedGroup.Name, actualQueryResult.Name)

	//upsert should fail when revision is less than the value in the db
	updatedGroup = group{Id: myGroup.Id, Name: "updatedGroup4", Revision: 1}
	_, err = DataStore.Upsert(cokeAdminCtx, updatedGroup)
	assert.ErrorIs(err, RevisionConflictError)

}
