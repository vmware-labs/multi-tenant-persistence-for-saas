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

package datastore_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/bxcodec/faker/v4"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/test"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/test/pb"
)

var LOG *logrus.Entry

// TODO - add a test that would show that the DB users are not able to create, drop, or truncate tables

func TestHasTable(t *testing.T) {
	assert := assert.New(t)

	ds, _ := SetupDataStore("TestTruncate")
	defer ds.Reset()
	_, _, _ = SetupDbTables(ds)

	exists, err := ds.TestHelper().HasTable("non-existent-table")
	assert.NoError(err)
	assert.False(exists, "Expected non-existent table not to be found ")

	exists, err = ds.TestHelper().HasTable(datastore.GetTableName(App{}))
	assert.NoError(err)
	assert.True(exists, "Expected existing table to be found ")
}

func TestTruncate(t *testing.T) {
	assert := assert.New(t)

	ds, _ := SetupDataStore("TestTruncate")
	defer ds.Reset()
	_, _, _ = SetupDbTables(ds)

	queryResults := make([]App, 0)
	if err := ds.FindAll(CokeAdminCtx, &queryResults, datastore.DefaultPagination()); err != nil {
		assert.FailNow(fmt.Sprintf("Failed to query DB table %s: %s", datastore.GetTableName(App{}), err.Error()))
	} else if len(queryResults) == 0 {
		assert.FailNow("Failed to set up test case")
	}

	err := ds.TestHelper().Truncate(datastore.GetTableName(App{}))
	assert.NoError(err)

	queryResults = make([]App, 0)
	err = ds.FindAll(CokeAdminCtx, &queryResults, datastore.DefaultPagination())
	assert.NoError(err)
	assert.Empty(queryResults, "Expected all records to be deleted after table truncate")
}

func TestTruncateNonExistent(t *testing.T) {
	assert := assert.New(t)
	ds, _ := SetupDataStore("TestTruncateNonExistent")
	defer ds.Reset()
	err := ds.TestHelper().Truncate("non_existent_table")
	assert.NoError(err, "Expected no error when trying to truncate a non-existent table")
}

func TestFindInEmpty(t *testing.T) {
	assert := assert.New(t)

	ds, _ := SetupDataStore("TestFindInEmpty")
	defer ds.Reset()
	RecreateAllTables(ds)

	{
		queryResult := AppUser{Id: "non-existent user ID"}
		err := ds.Find(CokeAdminCtx, &queryResult)
		assert.ErrorIs(err, ErrRecordNotFound)
		assert.True(queryResult.AreNonKeyFieldsEmpty())
	}

	{
		records := make([]AppUser, 0)
		err := ds.FindAll(CokeAdminCtx, &records, datastore.DefaultPagination())
		assert.NoError(err)
		assert.Empty(records)
	}

	{
		records := make([]App, 0)
		err := ds.FindAll(CokeAdminCtx, &records, datastore.DefaultPagination())
		assert.NoError(err)
		assert.Empty(records)
	}
}

func testCrud(t *testing.T, ds datastore.DataStore, ctx context.Context, myCokeApp *App, user1, user2 *AppUser) {
	t.Helper()
	assert := assert.New(t)
	var err error

	// Querying of previously inserted records should succeed
	for _, record := range []*AppUser{user1, user2} {
		queryResult := AppUser{Id: record.Id}
		err = ds.Find(ctx, &queryResult)
		assert.NoError(err)
		assert.Equal(record, &queryResult)
	}

	// Updating non-key fields in a record should succeed
	user1.Name = "Jeyhun G."
	user1.Email = "jeyhun111@mail.com"
	user1.EmailConfirmed = !user1.EmailConfirmed
	user1.NumFollowers++
	user2.Name = "Jahangir G."
	user2.Email = "jahangir111@mail.com"
	user2.EmailConfirmed = !user2.EmailConfirmed
	user2.NumFollowers--
	for _, record := range []*AppUser{user1, user2} {
		rowsAffected, err := ds.Update(ctx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)
		queryResult := &AppUser{Id: record.Id}
		err = ds.Find(ctx, queryResult)
		assert.NoError(err)
		assert.Equal(record, queryResult)
	}

	// Upsert operation should be an update for already existing records
	user1.NumFollowers++
	user2.NumFollowers--
	for _, record := range []*AppUser{user1, user2} {
		rowsAffected, err := ds.Update(ctx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)
		queryResult := &AppUser{Id: record.Id}
		err = ds.Find(ctx, queryResult)
		assert.NoError(err)
		assert.Equal(record, queryResult)
	}

	// Deletion of existing records should not fail, and the records should no longer be found in the DB
	for _, record := range []*AppUser{user1, user2} {
		rowsAffected, err := ds.Delete(ctx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)
		queryResult := AppUser{Id: record.Id}
		err = ds.Find(ctx, &queryResult)
		assert.ErrorIs(err, ErrRecordNotFound)
		assert.True(queryResult.AreNonKeyFieldsEmpty())
	}
}

func TestMain(m *testing.M) {
	LOG = datastore.GetCompLogger()

	code := m.Run()
	os.Exit(code)
}

func BenchmarkCrud(b *testing.B) {
	logger := datastore.GetLogger()
	logger.SetLevel(logrus.FatalLevel)
	logger.SetOutput(io.Discard)
	LOG = logger.WithField(datastore.COMP, datastore.SAAS_PERSISTENCE)

	var t testing.T
	ds, _ := SetupDataStore("BenchmarkCrud")
	defer ds.Reset()
	myCokeApp, user1, user2 := SetupDbTables(ds)
	for n := 0; n < b.N; n++ {
		testCrud(&t, ds, CokeAdminCtx, myCokeApp, user1, user2)
	}
}

func TestCrud(t *testing.T) {
	ds, _ := SetupDataStore("TestCrud")
	defer ds.Reset()
	myCokeApp, user1, user2 := SetupDbTables(ds)
	testCrud(t, ds, CokeAdminCtx, myCokeApp, user1, user2)
}

func TestFindAll(t *testing.T) {
	assert := assert.New(t)
	ds, _ := SetupDataStore("TestFindAll")
	defer ds.Reset()
	_, user1, user2 := SetupDbTables(ds)

	// FindAll should return all (two) records
	queryResults := make([]AppUser, 0)
	err := ds.FindAll(CokeAdminCtx, &queryResults, datastore.DefaultPagination())
	sort.Sort(AppUserSlice(queryResults))
	assert.NoError(err)
	assert.Len(queryResults, 2)

	expected := []*AppUser{user1, user2}

	for i := 0; i < len(queryResults); i++ {
		assert.Equal(expected[i], &queryResults[i])
	}
}

func myID(i int) string {
	return fmt.Sprintf("id-%04d", i)
}

func TestFindAllWithPagination(t *testing.T) {
	assert := assert.New(t)
	ds, _ := SetupDataStore("TestFindAllWithPagination")
	defer ds.Reset()
	RecreateAllTables(ds)

	a1 := &App{}
	for i := 0; i < 100; i++ {
		_ = faker.FakeData(a1)
		a1.Id = myID(i)
		a1.TenantId = COKE
		rowsAffected, err := ds.Insert(CokeAdminCtx, a1)
		assert.NoError(err)
		assert.Equal(int64(1), rowsAffected)
	}
	// FindAll should return all (two) records
	queryResults := make([]App, 0)
	err := ds.FindAll(CokeAdminCtx, &queryResults, datastore.DefaultPagination())
	assert.NoError(err)
	assert.Len(queryResults, 100)

	for i := 0; i < len(queryResults); i++ {
		assert.Equal(myID(i), queryResults[i].Id)
	}

	i := 0
	for {
		queryResults := make([]App, 0)
		err = ds.FindAll(CokeAdminCtx, &queryResults, datastore.GetPagination(i, 1, "application_id desc"))
		if i == 100 {
			assert.NoError(err)
			assert.Len(queryResults, 0)
			break
		}
		t.Logf("Verifying entry[%+v] with %+v", i, queryResults[0])
		assert.NoError(err)
		assert.Len(queryResults, 1)
		assert.Equal(myID(99-i), queryResults[0].Id)
		i += 1
	}
}

func TestFindWithCriteria(t *testing.T) {
	t.Helper()
	assert := assert.New(t)
	ds, _ := SetupDataStore("TestFindWithCriteria")
	defer ds.Reset()
	_, user1, user2 := SetupDbTables(ds)

	expected := []*AppUser{user1, user2}

	// Pass filtering criteria
	for _, user := range expected {
		// Search by all fields
		queryResults := make([]AppUser, 0)
		err := ds.FindWithFilter(CokeAdminCtx, user, &queryResults, datastore.NoPagination())
		assert.NoError(err)
		assert.Len(queryResults, 1)
		assert.Equal(user, &queryResults[0])

		// Search only by name
		queryResults = make([]AppUser, 0)
		err = ds.FindWithFilter(CokeAdminCtx, &AppUser{Name: user.Name}, &queryResults, datastore.NoPagination())
		assert.NoError(err)
		assert.Len(queryResults, 1)
		assert.Equal(user, &queryResults[0])
	}
}

func TestCrudWithMissingOrgId(t *testing.T) {
	assert := assert.New(t)

	ds, _ := SetupDataStore("TestCrudWithMissingOrgId")
	defer ds.Reset()
	RecreateAllTables(ds)
	_, apps := make([]AppUser, 0), make([]App, 0)

	// Insert some data, to make sure that DAL methods fail not due to data missing in data store
	rowsAffected, err := ds.Insert(CokeAdminCtx, &AppUser{Id: RANDOM_ID})
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)

	// FIND ALL
	err = ds.FindAll(context.Background(), &apps, datastore.DefaultPagination()) // Missing org. ID in context
	assert.ErrorIs(err, ErrFetchingMetadata)

	// FIND BY ID
	err = ds.Find(context.Background(), &App{Id: "random ID"}) // Missing org. ID in context
	assert.ErrorIs(err, ErrFetchingMetadata)

	// DELETE
	_, err = ds.Delete(context.Background(), &App{Id: "random ID"}) // Missing org. ID in context
	assert.ErrorIs(err, ErrFetchingMetadata)

	// INSERT
	rowsAffected, err = ds.Insert(context.Background(), &App{Id: RANDOM_ID}) // Missing org. ID in context
	assert.ErrorIs(err, ErrFetchingMetadata)
	assert.Equal(int64(0), rowsAffected)

	// UPDATE
	_, err = ds.Update(context.Background(), &App{Id: RANDOM_ID}) // Missing org. ID in context
	assert.ErrorIs(err, ErrFetchingMetadata)

	_, err = ds.Delete(CokeAdminCtx, &AppUser{Id: RANDOM_ID})
	assert.NoError(err)
}

func TestCrudWithInvalidParams(t *testing.T) {
	t.Helper()
	assert := assert.New(t)
	ds, _ := SetupDataStore("TestCrudWithInvalidParams")
	defer ds.Reset()
	RecreateAllTables(ds)
	ctx := CokeAdminCtx

	// Insert some data, to make sure that DAL methods fail not due to data missing in data store
	rowsAffected, err := ds.Insert(ctx, &AppUser{Id: RANDOM_ID})
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)

	// FIND ALL
	var apps []App
	err = ds.FindAll(ctx, apps, datastore.NoPagination()) // Passing a nil slice
	assert.ErrorIs(err, ErrNotPtrToStructSlice)

	apps = make([]App, 0)

	// FIND ALL
	err = ds.FindAll(ctx, apps, datastore.NoPagination()) // Passing slice by value
	assert.ErrorIs(err, ErrNotPtrToStructSlice)

	// FIND ALL
	err = ds.FindAll(ctx, &App{}, datastore.NoPagination()) // Passing a struct by reference (instead of a slice of structs by reference)
	assert.ErrorIs(err, ErrNotPtrToStructSlice)

	// FIND BY ID
	err = ds.Find(ctx, AppUser{}) // Passing a struct instead of a pointer to a struct
	assert.ErrorIs(err, ErrNotPtrToStruct)

	_, err = ds.Insert(CokeAuditorCtx, &App{TenantId: COKE, Id: "foo"})
	assert.ErrorIs(err, ErrExecutingSqlStmt)

	_, err = ds.Insert(PepsiAuditorCtx, &AppUser{Id: "foo"})
	assert.ErrorIs(err, ErrExecutingSqlStmt)
}

func TestDALRegistration(t *testing.T) {
	t.Helper()
	assert := assert.New(t)
	ds, _ := SetupDataStore("TestDALRegistration")
	defer ds.Reset()

	roleMapping := map[string]dbrole.DbRole{SERVICE_AUDITOR: dbrole.READER}
	// When registering a struct with DAL, you should be able to pass it either by value or by reference
	err := ds.Register(CokeAdminCtx, roleMapping, App{})
	assert.NoError(err)

	err = ds.Register(CokeAdminCtx, roleMapping, &AppUser{})
	assert.NoError(err)

	// You should be able to register a struct that happens to have a name that's a reserved keyword in Postgres
	err = ds.Register(CokeAdminCtx, roleMapping, &Group{})
	assert.NoError(err)

	// Reset DB connections. Check if Register() is still able to reconnect to DB
	ds.Reset()

	roleMapping = map[string]dbrole.DbRole{SERVICE_AUDITOR: dbrole.READER}
	// When registering a struct with DAL, you should be able to pass it either by value or by reference
	err = ds.Register(CokeAdminCtx, roleMapping, App{})
	assert.NoError(err)

	err = ds.Register(CokeAdminCtx, roleMapping, &AppUser{})
	assert.NoError(err)

	// You should be able to register a struct that happens to have a name that's a reserved keyword in Postgres
	err = ds.Register(CokeAdminCtx, roleMapping, &Group{})
	assert.NoError(err)
}

/*
Checks if DAL is able to select the least restrictive available DB role to perform SQL operations on one table.
*/
func TestDeleteWithMultipleCSPRoles(t *testing.T) {
	const APP_ADMIN = "app_admin"
	assert := assert.New(t)
	ds, _ := SetupDataStore("TestDeleteWithMultipleCSPRoles")
	defer ds.Reset()

	// Create context for custom admin who will have 2 service roles
	customCtx := ds.GetAuthorizer().GetAuthContext(COKE, APP_ADMIN, SERVICE_ADMIN)

	// The custom admin will have a read access being an app admin and read & write access being a service admin
	roleMapping := map[string]dbrole.DbRole{APP_ADMIN: dbrole.READER, SERVICE_ADMIN: dbrole.WRITER}
	err := ds.Register(customCtx, roleMapping, App{})
	if err != nil {
		assert.FailNow("Failed to setup the test case for the following reason:\n")
	}

	// Add some data to the app table
	myCokeApp, _, _ := PrepareInput()
	if _, err = ds.Insert(CokeAdminCtx, myCokeApp); err != nil {
		assert.FailNow("Failed to prepare data for the test case for the following reason:\n" + err.Error())
	}

	// Make sure that the custom admin is able to delete the data
	rowsAffected, err := ds.Delete(customCtx, myCokeApp)
	assert.NoError(err)
	assert.EqualValues(1, rowsAffected)
}

/*
Tries to perform a query with a service role that has not been authorized to access the table.
*/
func TestUnauthorizedAccess(t *testing.T) {
	assert := assert.New(t)
	ds, _ := SetupDataStore("TestUnauthorizedAccess")
	defer ds.Reset()

	myCokeApp, _, _ := SetupDbTables(ds)
	ctx := ds.GetAuthorizer().GetAuthContext(COKE, "unauthorized service role")
	queryResult := App{Id: myCokeApp.Id, TenantId: PEPSI}
	err := ds.Find(ctx, &queryResult)
	assert.ErrorIs(err, ErrOperationNotAllowed)
}

/*
Tries CRUD operations with Pepsi's org ID in the context, while the data in DB belongs to Coke.
*/
func TestCrudWithMismatchingOrgId(t *testing.T) {
	t.Helper()
	assert := assert.New(t)
	ds, _ := SetupDataStore("TestCrudWithMismatchingOrgId")
	defer ds.Reset()
	myCokeApp, _, _ := SetupDbTables(ds)
	tenantStr := "tenant=Pepsi"
	orgIdStr := "orgIdCol=Coke"

	{
		queryResult := App{Id: myCokeApp.Id, TenantId: myCokeApp.TenantId}
		err := ds.Find(PepsiAdminCtx, &queryResult)
		assert.ErrorIs(err, ErrOperationNotAllowed) // Trying to read another tenant's data should return an error
		assert.True(strings.Contains(err.Error(), tenantStr), err.Error())
		assert.True(strings.Contains(err.Error(), orgIdStr), err.Error())
	}

	{
		queryResult := make([]App, 0)
		err := ds.FindAll(PepsiAdminCtx, &queryResult, datastore.NoPagination())
		assert.NoError(err)       // Pepsi tried to read all the records in DB - no error should be returned
		assert.Empty(queryResult) // But Pepsi should not see Coke's data

		// Try to read a specific record from DB that definitely exists but belongs to another tenant
		queryResult = make([]App, 0)
		err = ds.FindWithFilter(PepsiAdminCtx, myCokeApp, &queryResult, datastore.NoPagination())
		assert.ErrorIs(err, ErrOperationNotAllowed)
		assert.True(strings.Contains(err.Error(), tenantStr))
		assert.True(strings.Contains(err.Error(), orgIdStr))
	}

	{
		_, err := ds.Delete(PepsiAdminCtx, myCokeApp)
		assert.ErrorIs(err, ErrOperationNotAllowed) // Trying to delete another tenant's data should return an error
		queryResult := App{Id: myCokeApp.Id, TenantId: myCokeApp.TenantId}
		err = ds.Find(CokeAdminCtx, &queryResult) // Since the previous delete has failed, the data should still be in the DB
		assert.NoError(err)
		assert.NotEmpty(queryResult)
	}

	{
		newApp := App{
			Id:       "id-" + strconv.Itoa(rand.Int()),
			Name:     "New app",
			TenantId: COKE,
		}

		// You should not be able to insert another tenant's data into data store
		rowsAffected, err := ds.Insert(PepsiAdminCtx, &newApp)
		assert.ErrorIs(err, ErrOperationNotAllowed)
		assert.Equal(int64(0), rowsAffected)

		// Another tenant's data should not be found because it should not have been inserted into the data store
		queryResult := App{Id: newApp.Id, TenantId: newApp.TenantId}
		err = ds.Find(CokeAdminCtx, &queryResult)
		assert.ErrorIs(err, ErrRecordNotFound)
		assert.True(queryResult.AreNonKeyFieldsEmpty())
	}

	{
		// App without an org. ID
		newApp := &App{
			Id:   "id-" + strconv.Itoa(rand.Int()),
			Name: "New app",
		}

		// You should not be able to insert a "multi-tenant" record that lacks the org. ID
		rowsAffected, err := ds.Insert(PepsiAdminCtx, newApp)
		assert.ErrorIs(err, ErrExecutingSqlStmt)
		assert.Equal(int64(0), rowsAffected)
	}

	{
		updatedApp := &App{
			Id:       myCokeApp.Id,
			Name:     "new name",
			TenantId: myCokeApp.TenantId,
		}
		_, err := ds.Update(PepsiAdminCtx, updatedApp) // You shouldn't be able to update another tenant's data with a tenant-specific role
		assert.ErrorIs(err, ErrOperationNotAllowed)

		queryResult := App{Id: myCokeApp.Id, TenantId: myCokeApp.TenantId}
		err = ds.Find(CokeAdminCtx, &queryResult)
		assert.NoError(err)
		assert.NotEqual(updatedApp.Name, queryResult.Name) // Record should not have been updated
		assert.Equal(myCokeApp.Name, queryResult.Name)     // Record should not have been updated
	}
}

func TestWithMissingEnvVar(t *testing.T) {
	assert := assert.New(t)

	for _, envVar := range []string{
		datastore.DB_ADMIN_USERNAME_ENV_VAR, datastore.DB_ADMIN_PASSWORD_ENV_VAR,
		datastore.DB_NAME_ENV_VAR, datastore.DB_PORT_ENV_VAR, datastore.DB_HOST_ENV_VAR, datastore.SSL_MODE_ENV_VAR,
	} {
		defer os.Setenv(envVar, os.Getenv(envVar))
		os.Unsetenv(envVar)
		assert.Panics(func() { _, _ = datastore.FromEnv(LOG, TestMetadataAuthorizer, authorizer.SimpleInstancer{}) })
	}
}

func TestWithEmptyEnvVar(t *testing.T) {
	assert := assert.New(t)

	for _, envVar := range []string{
		datastore.DB_ADMIN_USERNAME_ENV_VAR, datastore.DB_ADMIN_PASSWORD_ENV_VAR,
		datastore.DB_NAME_ENV_VAR, datastore.DB_PORT_ENV_VAR, datastore.DB_HOST_ENV_VAR, datastore.SSL_MODE_ENV_VAR,
	} {
		defer os.Setenv(envVar, os.Getenv(envVar))
		os.Setenv(envVar, "")
		assert.Panics(func() { _, _ = datastore.FromEnv(LOG, TestMetadataAuthorizer, authorizer.SimpleInstancer{}) })
	}
}

func TestWithBlankEnvVar(t *testing.T) {
	assert := assert.New(t)

	for _, envVar := range []string{
		datastore.DB_ADMIN_USERNAME_ENV_VAR, datastore.DB_ADMIN_PASSWORD_ENV_VAR,
		datastore.DB_NAME_ENV_VAR, datastore.DB_PORT_ENV_VAR, datastore.DB_HOST_ENV_VAR, datastore.SSL_MODE_ENV_VAR,
	} {
		defer os.Setenv(envVar, os.Getenv(envVar))
		os.Setenv(envVar, "    ")
		assert.Panics(func() { _, _ = datastore.FromEnv(LOG, TestMetadataAuthorizer, authorizer.SimpleInstancer{}) })
	}
}

/*
Checks that you can correctly sort DB roles based how restrictive they are (most restrictive to the least restrictive roles).
E.g., tenant_reader more restrictive than reader.
*/
func TestDbRoleSorting(t *testing.T) {
	assert := assert.New(t)

	var roles dbrole.DbRoleSlice = []dbrole.DbRole{dbrole.WRITER, dbrole.TENANT_WRITER, dbrole.READER, dbrole.TENANT_READER}
	writerIndex, tenantWriterIndex, readerIndex, tenantReaderIndex := 0, 1, 2, 3
	assert.False(roles.Less(writerIndex, tenantWriterIndex))
	assert.False(roles.Less(tenantWriterIndex, readerIndex))
	assert.False(roles.Less(readerIndex, tenantReaderIndex))
	assert.False(roles.Less(writerIndex, readerIndex))

	sort.Sort(roles)
	assert.True(roles.Less(0, 3))
}

/*
Tests revision blocking updates that are outdated.
*/
func TestRevision(t *testing.T) {
	assert := assert.New(t)
	ds, _ := SetupDataStore("TestRevision")
	defer ds.Reset()

	if err := ds.TestHelper().DropTables(&Group{}); err != nil {
		assert.FailNow("Failed to drop DB tables for the following reason:\n" + err.Error())
	}
	roleMapping := map[string]dbrole.DbRole{
		TENANT_AUDITOR:  dbrole.TENANT_READER,
		TENANT_ADMIN:    dbrole.TENANT_WRITER,
		SERVICE_AUDITOR: dbrole.READER,
		SERVICE_ADMIN:   dbrole.WRITER,
	}

	err := ds.Register(CokeAdminCtx, roleMapping, Group{})
	assert.NoError(err)
	myGroup := Group{Id: "withRevisionId-12345", Name: "withRevisionName", InstanceId: AMERICAS}

	// FIXME Initial revision should be 1
	const initialRevision int = 0

	_, err = ds.Insert(AmericasCokeAdminCtx, &myGroup)
	assert.NoError(err)

	actualQueryResult, expectedQueryResult := Group{Id: myGroup.Id}, Group{Id: "withRevisionId-12345", Name: "withRevisionName", Revision: initialRevision, InstanceId: AMERICAS}
	err = ds.Find(AmericasCokeAdminCtx, &actualQueryResult)
	assert.NoError(err)
	assert.Equal(expectedQueryResult, actualQueryResult)

	actualQueryResult, expectedQueryResult = Group{Id: myGroup.Id}, Group{Id: "withRevisionId-12345", Name: "withRevisionName", Revision: initialRevision, InstanceId: AMERICAS}
	err = ds.Find(EuropeCokeAdminCtx, &actualQueryResult)
	assert.NotEqual(expectedQueryResult, actualQueryResult)
	assert.ErrorIs(err, ErrRecordNotFound)

	// update should  succeed when revision is the same as the value in the db
	updatedGroup := Group{Id: myGroup.Id, Name: "updatedGroup", Revision: initialRevision, InstanceId: AMERICAS}
	rowsAffected, err := ds.Update(AmericasCokeAdminCtx, &updatedGroup)
	assert.NoError(err)
	assert.EqualValues(1, rowsAffected)
	actualQueryResult = Group{Id: myGroup.Id}
	err = ds.Find(AmericasCokeAdminCtx, &actualQueryResult)
	assert.NoError(err)
	assert.Equal(initialRevision+1, actualQueryResult.Revision, "Expected revision to be incremented by 1 after update")
	assert.Equal(updatedGroup.Name, actualQueryResult.Name)

	// update operation should not succeed if the instanceId dont match with context
	rowsAffected, err = ds.Update(EuropeCokeAdminCtx, &updatedGroup)
	assert.ErrorIs(err, ErrOperationNotAllowed)
	assert.EqualValues(0, rowsAffected)
	rowsAffected, err = ds.Update(EuropeCokeAuditorCtx, &updatedGroup)
	assert.ErrorIs(err, ErrOperationNotAllowed)
	assert.EqualValues(0, rowsAffected)

	// upsert should  succeed when revision is the same as the value in the db
	updatedGroup = Group{Id: myGroup.Id, Name: "updatedGroup2", Revision: actualQueryResult.Revision, InstanceId: AMERICAS}
	rowsAffected, err = ds.Upsert(AmericasCokeAdminCtx, &updatedGroup)
	assert.NoError(err)
	assert.EqualValues(1, rowsAffected)
	actualQueryResult = Group{Id: myGroup.Id}
	err = ds.Find(AmericasCokeAdminCtx, &actualQueryResult)
	assert.NoError(err)
	assert.Equal(initialRevision+2, actualQueryResult.Revision, "Expected revision to be incremented by 1 after upsert")
	assert.Equal(updatedGroup.Name, actualQueryResult.Name)

	// update should fail when revision is not equal to the value in the db
	updatedGroup = Group{Id: myGroup.Id, Name: "updatedGroup3", Revision: initialRevision + 100, InstanceId: AMERICAS}
	rowsAffected, err = ds.Update(AmericasCokeAdminCtx, &updatedGroup)
	assert.ErrorIs(err, ErrRevisionConflict)
	assert.EqualValues(0, rowsAffected)
	actualQueryResult = Group{Id: myGroup.Id}
	err = ds.Find(AmericasCokeAdminCtx, &actualQueryResult)
	assert.NoError(err)
	assert.NotEqual(updatedGroup.Revision+1, actualQueryResult.Revision)
	assert.NotEqual(updatedGroup.Name, actualQueryResult.Name)

	// upsert should fail when revision is not equal to the value in the db
	updatedGroup = Group{Id: myGroup.Id, Name: "updatedGroup4", Revision: initialRevision + 100, InstanceId: AMERICAS}
	_, err = ds.Upsert(AmericasCokeAdminCtx, &updatedGroup)
	assert.ErrorIs(err, ErrRevisionConflict)
	actualQueryResult = Group{Id: myGroup.Id}
	err = ds.Find(AmericasCokeAdminCtx, &actualQueryResult)
	assert.NoError(err)
	assert.NotEqual(updatedGroup.Revision+1, actualQueryResult.Revision)
	assert.NotEqual(updatedGroup.Name, actualQueryResult.Name)

	// upsert should fail when revision is less than the value in the db
	updatedGroup = Group{Id: myGroup.Id, Name: "updatedGroup4", Revision: 1, InstanceId: AMERICAS}
	rowsAffected, err = ds.Upsert(AmericasCokeAdminCtx, &updatedGroup)
	assert.ErrorIs(err, ErrRevisionConflict)
	assert.EqualValues(0, rowsAffected)

	rowsAffected, err = ds.Delete(AmericasCokeAuditorCtx, &updatedGroup)
	assert.ErrorIs(err, ErrExecutingSqlStmt)
	assert.EqualValues(0, rowsAffected)

	rowsAffected, err = ds.Delete(EuropeCokeAuditorCtx, &updatedGroup)
	assert.ErrorIs(err, ErrOperationNotAllowed)
	assert.EqualValues(0, rowsAffected)

	rowsAffected, err = ds.Delete(AmericasCokeAdminCtx, &updatedGroup)
	assert.NoError(err)
	assert.EqualValues(1, rowsAffected)
}

func TestTransactions(t *testing.T) {
	assert := assert.New(t)
	ds, ps := SetupDataStore("TestTransactions")
	defer ds.Reset()
	_, _, _ = SetupDbTables(ds)
	roleMapping := map[string]dbrole.DbRole{
		TENANT_AUDITOR:  dbrole.TENANT_READER,
		TENANT_ADMIN:    dbrole.TENANT_WRITER,
		SERVICE_AUDITOR: dbrole.READER,
		SERVICE_ADMIN:   dbrole.WRITER,
	}
	err := ps.Register(context.TODO(), roleMapping, &pb.Disk{})
	assert.NoError(err)

	testSingleTableTransactions(t, ds, ps)
	testMultiTableTransactions(t, ds, ps)
	testMultiProtoTransactions(t, ds, ps)
}
