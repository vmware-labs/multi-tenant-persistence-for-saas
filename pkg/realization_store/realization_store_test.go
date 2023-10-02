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

package realization_store_test

import (
	"context"
	"database/sql"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bxcodec/faker/v4"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/protostore"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/realization_store"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/test"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/test/pb"
)

type StatusPair struct {
	expected Status
	actual   Status
}

func checkStatuses(t *testing.T, statusList []StatusPair) {
	t.Helper()
	for _, pair := range statusList {
		assert.Equal(t, pair.expected.String(), pair.actual.String())
	}
}

func checkStatus(t *testing.T, expected, actual Status, msgAndArgs ...interface{}) {
	t.Helper()
	assert.Equal(t, expected.String(), actual.String(), msgAndArgs...)
}

func setDalDbEnv() {
	const (
		host     = "localhost"
		port     = 5432
		user     = "postgres"
		password = "postgres"
		dbname   = "tmp_db_"
		sslmode  = "disable"
		logLevel = ""
	)

	_ = os.Setenv("DB_ADMIN_USERNAME", user)
	_ = os.Setenv("DB_ADMIN_PASSWORD", password)
	_ = os.Setenv("DB_NAME", dbname)
	_ = os.Setenv("DB_PORT", strconv.Itoa(port))
	_ = os.Setenv("DB_HOST", host)
	_ = os.Setenv("SSL_MODE", sslmode)
	_ = os.Setenv("LOG_LEVEL", logLevel)
}

func TestMain(m *testing.M) {
	setDalDbEnv()
	InitTestData("realization_store_test")
	exitVal := m.Run()
	os.Exit(exitVal)
}

var RSDbTableNames = []string{
	GetOverallStatusTableName(&pb.CPU{}),
	GetEnforcementStatusTableName(&pb.CPU{}),
	datastore.GetTableName(&pb.CPU{}),
}

func getSampleCPU() *ProtobufWithMetadata {
	id := "Intel-" + faker.Username()
	cpu := ProtobufWithMetadata{
		Message: &pb.CPU{
			Brand:         "Intel",
			Name:          "i9-9900K",
			NumberCores:   8,
			NumberThreads: 16,
			MinGhz:        1.0,
			MaxGhz:        2.0,
		},
		Metadata: protostore.Metadata{
			Id:       id,
			Revision: 1,
		},
	}

	return &cpu
}

func setupRealizationStore(t *testing.T, orgId, userRole string) context.Context {
	t.Helper()
	assert := assert.New(t)
	ctx := TestAuthorizer.GetAuthContext(orgId, userRole)

	err := DS.TestHelper().Truncate(RSDbTableNames...)
	if err != nil {
		assert.FailNow("Failed to truncate realization store DB tables", err)
	}

	err = RS.Register(ctx, map[string]dbrole.DbRole{userRole: dbrole.TENANT_WRITER}, &pb.CPU{})
	if err != nil {
		assert.FailNow("Failed to register Protobuf record with RealizationStore", err)
	}

	return ctx
}

func TestUnreachableCode(t *testing.T) {
	for _, status := range []Status{UNKNOWN, PENDING, IN_PROGRESS, REALIZED, ERROR, DELETION_PENDING, DELETION_REALIZED, 5_000 /* will fall into "default" case */} {
		_ = status.String()
	}
}

func TestPassingEmptyContext(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	_ = setupRealizationStore(t, orgId, TENANT_ADMIN)
	cpu := getSampleCPU()
	assert.ErrorIs(RS.FindById(context.Background(), "", cpu), errors.ErrFetchingMetadata)

	_, _, err := RS.PersistIntent(context.Background(), cpu)
	assert.ErrorIs(err, errors.ErrFetchingMetadata)

	assert.ErrorIs(RS.MarkEnforcementAsPending(context.Background(), "", cpu), errors.ErrFetchingMetadata)
	assert.ErrorIs(RS.MarkEnforcementAsSuccess(context.Background(), "", cpu), errors.ErrFetchingMetadata)
	assert.ErrorIs(RS.MarkEnforcementAsError(context.Background(), "", "", cpu), errors.ErrFetchingMetadata)

	_, err = RS.Delete(context.Background(), cpu)
	assert.ErrorIs(err, errors.ErrFetchingMetadata)

	assert.ErrorIs(RS.MarkEnforcementAsDeletionPending(context.Background(), "", cpu), errors.ErrFetchingMetadata)
	assert.ErrorIs(RS.MarkEnforcementAsDeletionRealized(context.Background(), "", cpu), errors.ErrFetchingMetadata)

	_, err = RS.GetOverallStatus(context.Background(), cpu)
	assert.ErrorIs(err, errors.ErrFetchingMetadata)

	_, _, err = RS.GetOverallStatusWithEnforcementDetails(context.Background(), cpu)
	assert.ErrorIs(err, errors.ErrFetchingMetadata)
}

func TestRealizationStoreRegistration(t *testing.T) {
	assert := assert.New(t)
	for _, tableName := range RSDbTableNames {
		if err := DS.TestHelper().DropTables(tableName); err != nil {
			assert.FailNow("Failed to drop DB table(s)", err)
		}
	}

	err := RS.Register(context.TODO(), nil, &pb.CPU{})
	assert.NoError(err)

	for _, tableName := range RSDbTableNames {
		if err = DS.TestHelper().DropTables(tableName); err != nil {
			assert.FailNow("Failed to drop DB table(s)", err)
		}
	}
}

func TestFindByIdNonExistent(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, SERVICE_ADMIN)

	queryResults := ProtobufWithMetadata{new(pb.CPU), protostore.Metadata{}}
	err := RS.FindById(ctx, "non-existent-resource", &queryResults)
	assert.ErrorIs(err, errors.ErrRecordNotFound)
}

func areMetadataEqual(assert *assert.Assertions, resource *protostore.Metadata, updated *protostore.Metadata) {
	resource.CreatedAt = updated.CreatedAt
	resource.UpdatedAt = updated.UpdatedAt
	assert.Equal(*resource, *updated)
}

/*
Persists cpu the given number of times in realization store.
Confirms that the resource is persisted successfully each time, that overall status is set to PENDING, and that
revision is consistent in both resource's table and overall status table.
*/
func testPersistIntent(t *testing.T, ctx context.Context, cpu *ProtobufWithMetadata, numTimes int) {
	t.Helper()
	assert := assert.New(t)

	const initRevision = 1
	for i := 0; i < numTimes; i++ {
		numThreads := rand.New(rand.NewSource(time.Now().UnixNano())).Uint32() % 10_000
		cpu.Message.(*pb.CPU).NumberThreads = numThreads // Perform small modification
		rowsAffected, metadata, err := RS.PersistIntent(ctx, cpu)
		assert.NoError(err, "RealizationStore's PersistIntent failed")
		assert.EqualValues(1, rowsAffected)
		assert.EqualValues(initRevision+i, metadata.Revision)
		assert.Equal(cpu.Id, metadata.Id)

		// Check that the upserted record is found
		queryResults := &ProtobufWithMetadata{new(pb.CPU), protostore.Metadata{}}
		err = RS.FindById(ctx, cpu.Id, queryResults)
		assert.NoError(err, "RealizationStore's FindById failed")
		assert.True(proto.Equal(cpu.Message, queryResults.Message))
		assert.Equal(numThreads, queryResults.Message.(*pb.CPU).NumberThreads, "Failed to persist Protobuf message")
		assert.EqualValues(initRevision+i, queryResults.Revision)

		cpu.Revision = metadata.Revision
		overallStatus, err := RS.GetOverallStatus(ctx, cpu)
		assert.NoError(err)
		checkStatus(t, PENDING, overallStatus)

		testResourceRevisionConsistency(t, ctx, cpu.Id, &pb.CPU{})
	}
}

/*
Checks that IRealizationStore.PersistIntent does not modify the intent argument before it is upserted into the DB.
*/
func TestPersistIntent(t *testing.T) {
	assert := assert.New(t)

	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()

	// Perform insertion
	cpu.Message.(*pb.CPU).NumberThreads = rand.New(rand.NewSource(time.Now().UnixNano())).Uint32() % 10_000
	before := proto.Clone(cpu)
	_, _, err := RS.PersistIntent(ctx, cpu) // Acts as insertion
	assert.NoError(err)
	assert.True(proto.Equal(before, cpu.Message))

	// Perform update
	cpu.Message.(*pb.CPU).NumberThreads = rand.New(rand.NewSource(time.Now().UnixNano())).Uint32() % 10_000
	before = proto.Clone(cpu)

	_, _, err = RS.PersistIntent(ctx, cpu)
	assert.NoError(err)
	assert.True(proto.Equal(before, cpu.Message))
}

/*
Persists intent 10 times, modifying it each time.
*/
func TestPersistIntentMultipleTimes(t *testing.T) {
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()
	testPersistIntent(t, ctx, cpu, 10)
}

/*
Deletes a resource and marks it as pending for deletion at the given enforcement points.
*/
func stopEnforcement(t *testing.T, ctx context.Context, resource *ProtobufWithMetadata, enforcementPoints []string) {
	t.Helper()
	assert := assert.New(t)
	if rowsAffected, err := RS.Delete(ctx, resource); err != nil {
		assert.FailNow("Failed to delete resource", err)
	} else if rowsAffected != 1 {
		assert.FailNowf("Record %s not found", resource.Id)
	}
	for _, enforcementPoint := range enforcementPoints {
		if err := RS.MarkEnforcementAsDeletionPending(ctx, enforcementPoint, resource); err != nil {
			assert.FailNow("Failed to mark enforcement as deletion pending", err)
		}
	}
}

/*
- Truncates all DB tables
- Persists an intent 10 times (1 insertion & 9 updates) and marks it as PENDING at all the enforcements points passed in enforcementPointsFirstResource
- To simulate a resource being deleted, deletes it from DB & sets overall and enforcement status as DELETION_PENDING.
- Persists an intent, which reuses an ID of the previous intent, 10 times (1 insertion & 9 updates).
- Marks it as PENDING at all the enforcements points passed in enforcementPointsSecondResource.

The function checks that all times the DB is in a consistent state.
*/

func testPersisingtConflictingIntent(t *testing.T, enforcementPointsFirstResource []string, enforcementPointsSecondResource []string) {
	t.Helper()
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()
	testPersistIntent(t, ctx, cpu, 10)

	queryResults := ProtobufWithMetadata{Message: &pb.CPU{}}
	if err := RS.FindById(ctx, cpu.Id, &queryResults); err != nil {
		assert.FailNow("Failed to find resource by ID", err)
	}
	cpu.Revision = queryResults.Revision

	for _, enforcementPoint := range enforcementPointsFirstResource {
		if err := RS.MarkEnforcementAsPending(ctx, enforcementPoint, cpu); err != nil {
			assert.FailNow("Failed to mark enforcement as pending", err)
		}
	}
	testResourceRevisionConsistency(t, ctx, cpu.Id, &pb.CPU{}, enforcementPointsFirstResource...)

	// Delete the resource and mark its enforcement as DELETION_PENDING
	stopEnforcement(t, ctx, cpu, enforcementPointsFirstResource)

	cpu = getSampleCPU()
	testPersistIntent(t, ctx, cpu, 10)

	queryResults = ProtobufWithMetadata{Message: &pb.CPU{}}
	if err := RS.FindById(ctx, cpu.Id, &queryResults); err != nil {
		assert.FailNow("Failed to find resource by ID", err)
	}
	cpu.Revision = queryResults.Revision

	for _, enforcementPoint := range enforcementPointsSecondResource {
		if err := RS.MarkEnforcementAsPending(ctx, enforcementPoint, cpu); err != nil {
			assert.FailNow("Failed to mark enforcement as pending", err)
		}
	}
	testResourceRevisionConsistency(t, ctx, cpu.Id, &pb.CPU{}, enforcementPointsSecondResource...)
}

/*
Checks if you can persist an intent that reuses an ID of a previously deleted resource.
Assumes that there are no enforcement points at all.
*/
func TestPersistConflictingIntentNoEnforcementPoints(t *testing.T) {
	testPersisingtConflictingIntent(t, []string{}, []string{})
}

/*
Same as TestPersistConflictingIntentNoEnforcementPoints, but with an assumption that
the first and second intent are enforced at some enforcement points.
*/
func TestPersistConflictingIntent(t *testing.T) {
	awsUSEast, awsUSWest := []string{"us-east-1", "us-east-2"}, []string{"us-west-1", "us-west-2"}
	awsUS := make([]string, 0)
	awsUS = append(awsUS, awsUSEast...)
	awsUS = append(awsUS, awsUSWest...)

	azureUSEast, azureUSWest := []string{"eastus", "eastus2"}, []string{"westus", "westus2", "westus3"}
	azureUS := make([]string, 0)
	azureUS = append(azureUS, azureUSEast...)
	azureUS = append(azureUS, azureUSWest...)

	usWest := make([]string, 0)
	usWest = append(usWest, awsUSWest...)
	usWest = append(usWest, azureUSWest...)

	testPersisingtConflictingIntent(t, azureUS, azureUS)
	testPersisingtConflictingIntent(t, azureUS, awsUS)
	testPersisingtConflictingIntent(t, azureUS, usWest)
	testPersisingtConflictingIntent(t, usWest, azureUS)
}

/*
Negative test.
Checks that when persisting an intent, realization store won't clean up overall/enforcement status records of a resource that hasn't been deleted.
*/
func TestDeleteStaleStatusRecordsNegative(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()
	for i := 0; i < 2; i++ {
		if _, _, err := RS.PersistIntent(ctx, cpu); err != nil {
			assert.FailNow("Failed to persist an intent", err)
		}

		rowsAffected, err := RS.PurgeStaleStatusRecords(ctx, cpu)
		assert.NoError(err)
		assert.Zero(rowsAffected,
			"Expected no overall/enforcement status records to be considered stale as the resource has not been deleted")

		overallStatus, err := RS.GetOverallStatus(ctx, cpu)
		assert.NoError(err)
		checkStatus(t, PENDING, overallStatus,
			"Expected overall status to still be %s after it would fail to be deleted as a stale record", PENDING.String())
	}
}

func TestRealizationStoreUpsertInvalidRevision(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()

	// UPSERT ACTING AS INSERTION
	if _, _, err := RS.PersistIntent(ctx, cpu); err != nil {
		assert.FailNow("Failed to insert a record", err)
	}

	queryResults := ProtobufWithMetadata{new(pb.CPU), protostore.Metadata{}}
	if err := RS.FindById(ctx, cpu.Id, &queryResults); err != nil {
		assert.FailNow("Failed to find a record by ID", err)
	}

	// UPSERT ACTING AS UPDATE - use incorrect revision
	cpu.Message.(*pb.CPU).Brand = "AMD"
	cpu.Revision += queryResults.Revision + 1_000 // Set incorrect revision
	_, _, err := RS.PersistIntent(ctx, cpu)
	assert.ErrorIs(err, errors.ErrRevisionConflict, "Expected RealizationStore's Upsert to fail due to incorrect revision")
}

func TestRealizationStoreDelete(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()

	// UPSERT ACTING AS INSERTION
	if _, _, err := RS.PersistIntent(ctx, cpu); err != nil {
		assert.FailNow("Failed to insert a record", err)
	}

	rowsAffected, err := RS.Delete(ctx, cpu)
	assert.NoError(err)
	assert.EqualValues(1, rowsAffected)

	// Check that the resource has been deleted successfully
	queryResults := ProtobufWithMetadata{new(pb.CPU), protostore.Metadata{}}
	err = RS.FindById(ctx, cpu.Id, &queryResults)
	assert.ErrorIs(err, errors.ErrRecordNotFound, "Expected record not to be found after deletion")

	// Check that overall status is DELETION_REALIZED
	overallStatus, err := RS.GetOverallStatus(ctx, cpu)
	assert.NoError(err)
	checkStatus(t, DELETION_REALIZED, overallStatus)
}

func TestMarkEnforcementAsPending(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()
	id := cpu.Id

	// UPSERT ACTING AS INSERTION
	rowsAffected, metadata, err := RS.PersistIntent(ctx, cpu)
	assert.NoError(err, "RealizationStore's PersistIntent failed")
	assert.EqualValues(1, rowsAffected)
	areMetadataEqual(assert, &cpu.Metadata, &metadata)

	cpu = &ProtobufWithMetadata{new(pb.CPU), protostore.Metadata{}}
	err = RS.FindById(ctx, id, cpu)
	assert.NoError(err)

	enforcementPoints := [3]string{"cluster-001", "cluster-002", "cluster-003"}

	// Mark enforcement as pending
	// Try marking the same resource at the same enforcement point as PENDING multiple times.
	// The operation has to be idempotent
	for i := 0; i < 2; i++ {
		for _, enforcementPoint := range enforcementPoints {
			err = RS.MarkEnforcementAsPending(ctx, enforcementPoint, cpu)
			assert.NoError(err)
		}

		// Check if the enforcement statuses have been set to PENDING
		overallStatus, enforcementStatusMap, err := RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
		assert.NoError(err)
		checkStatus(t, PENDING, overallStatus)
		assert.Lenf(enforcementStatusMap, len(enforcementPoints), "Expected %d enforcement statuses to be returned", len(enforcementPoints))
		for _, enforcementStatus := range enforcementStatusMap {
			checkStatus(t, PENDING, enforcementStatus)
		}
	}

	testResourceRevisionConsistency(t, ctx, id, &pb.CPU{}, enforcementPoints[:]...)
}

func TestMarkEnforcementAsPendingOfNonExistentResource(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()

	err := RS.MarkEnforcementAsPending(ctx, "cluster-001", cpu)
	assert.ErrorIs(err, errors.ErrRecordNotFound)
}

func TestMarkEnforcementAsPendingWithIncorrectRevision(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	const id = "Intel-CPU"
	cpu := &ProtobufWithMetadata{
		Message: &pb.CPU{
			Brand:         "Intel",
			Name:          "i9-9900K",
			NumberCores:   8,
			NumberThreads: 16,
			MinGhz:        1.0,
			MaxGhz:        2.0,
		},
		Metadata: protostore.Metadata{
			Id:       id,
			Revision: 1,
		},
	}

	// UPSERT ACTING AS INSERTION
	if _, _, err := RS.PersistIntent(ctx, cpu); err != nil {
		assert.FailNow("Failed to upsert a record", err)
	}

	cpu.Revision += 1_000 // Set incorrect revision
	err := RS.MarkEnforcementAsPending(ctx, "cluster-001", cpu)
	assert.ErrorIs(err, errors.ErrRecordNotFound)
}

func TestRealizationWorkflow(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()
	id := cpu.Id

	t.Log("TestRealizationWorkflow started")
	_, _, err := RS.PersistIntent(ctx, cpu)
	assert.NoError(err)

	cpu2 := &ProtobufWithMetadata{new(pb.CPU), protostore.Metadata{}}
	err = RS.FindById(ctx, id, cpu2)
	assert.NoError(err)
	cpu.Metadata = cpu2.Metadata

	overallStatus, enforcementStatusMap, err := RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: PENDING, actual: overallStatus},
		{expected: UNKNOWN, actual: enforcementStatusMap["e1"]},
		{expected: UNKNOWN, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsInProgress(ctx, "e1", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: IN_PROGRESS, actual: overallStatus},
		{expected: IN_PROGRESS, actual: enforcementStatusMap["e1"]},
		{expected: UNKNOWN, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsInProgress(ctx, "e2", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: IN_PROGRESS, actual: overallStatus},
		{expected: IN_PROGRESS, actual: enforcementStatusMap["e1"]},
		{expected: IN_PROGRESS, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsSuccess(ctx, "e1", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: IN_PROGRESS, actual: overallStatus},
		{expected: REALIZED, actual: enforcementStatusMap["e1"]},
		{expected: IN_PROGRESS, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsError(ctx, "e2", "realization failed", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: ERROR, actual: overallStatus},
		{expected: REALIZED, actual: enforcementStatusMap["e1"]},
		{expected: ERROR, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsSuccess(ctx, "e2", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: REALIZED, actual: overallStatus},
		{expected: REALIZED, actual: enforcementStatusMap["e1"]},
		{expected: REALIZED, actual: enforcementStatusMap["e2"]},
	})

	cpu.Message.(*pb.CPU).Brand = "Intel(Updated)"
	_, _, err = RS.PersistIntent(ctx, cpu)
	assert.NoError(err)

	cpu2 = &ProtobufWithMetadata{new(pb.CPU), protostore.Metadata{}}
	err = RS.FindById(ctx, id, cpu2)
	assert.NoError(err)
	cpu.Metadata = cpu2.Metadata

	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: PENDING, actual: overallStatus},
		{expected: PENDING, actual: enforcementStatusMap["e1"]},
		{expected: PENDING, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsInProgress(ctx, "e1", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: PENDING, actual: overallStatus},
		{expected: IN_PROGRESS, actual: enforcementStatusMap["e1"]},
		{expected: PENDING, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsInProgress(ctx, "e2", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: IN_PROGRESS, actual: overallStatus},
		{expected: IN_PROGRESS, actual: enforcementStatusMap["e1"]},
		{expected: IN_PROGRESS, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsSuccess(ctx, "e1", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: IN_PROGRESS, actual: overallStatus},
		{expected: REALIZED, actual: enforcementStatusMap["e1"]},
		{expected: IN_PROGRESS, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsError(ctx, "e2", "realization failed", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: ERROR, actual: overallStatus},
		{expected: REALIZED, actual: enforcementStatusMap["e1"]},
		{expected: ERROR, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsSuccess(ctx, "e2", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: REALIZED, actual: overallStatus},
		{expected: REALIZED, actual: enforcementStatusMap["e1"]},
		{expected: REALIZED, actual: enforcementStatusMap["e2"]},
	})

	_, err = RS.SoftDelete(ctx, cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: DELETION_PENDING, actual: overallStatus},
		{expected: DELETION_PENDING, actual: enforcementStatusMap["e1"]},
		{expected: DELETION_PENDING, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsDeletionInProgress(ctx, "e1", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: DELETION_PENDING, actual: overallStatus},
		{expected: DELETION_IN_PROGRESS, actual: enforcementStatusMap["e1"]},
		{expected: DELETION_PENDING, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsDeletionInProgress(ctx, "e2", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: DELETION_IN_PROGRESS, actual: overallStatus},
		{expected: DELETION_IN_PROGRESS, actual: enforcementStatusMap["e1"]},
		{expected: DELETION_IN_PROGRESS, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsDeletionRealized(ctx, "e1", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: DELETION_IN_PROGRESS, actual: overallStatus},
		{expected: DELETION_REALIZED, actual: enforcementStatusMap["e1"]},
		{expected: DELETION_IN_PROGRESS, actual: enforcementStatusMap["e2"]},
	})

	/*
		TODO: Add support for marking deletion realization failures
		err = RS.MarkEnforcementAsError(ctx, "e2", "deletion failed", cpu)
		assert.NoError(err)
		overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
		assert.NoError(err)
		checkStatuses(t, []StatusPair{
			{expected: ERROR, actual: overallStatus},
			{expected: DELETION_REALIZED, actual: enforcementStatusMap["e1"]},
			{expected: ERROR, actual: enforcementStatusMap["e2"]},
		})
	*/

	err = RS.MarkEnforcementAsDeletionRealized(ctx, "e2", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: DELETION_REALIZED, actual: overallStatus},
		{expected: DELETION_REALIZED, actual: enforcementStatusMap["e1"]},
		{expected: DELETION_REALIZED, actual: enforcementStatusMap["e2"]},
	})

	/* TODO: Add support to full delete all enforcement and overall status records in this case */
	_, err = RS.Delete(ctx, cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: DELETION_REALIZED, actual: overallStatus},
		{expected: DELETION_REALIZED, actual: enforcementStatusMap["e1"]},
		{expected: DELETION_REALIZED, actual: enforcementStatusMap["e2"]},
	})

	_, err = RS.PurgeStaleStatusRecords(ctx, cpu)
	assert.NoError(err)
	_, _, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.ErrorIs(err, errors.ErrRecordNotFound)

	t.Log("TestRealizationWorkflow completed")
}

func TestPurgeStaleRecords(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()
	id := cpu.Id

	t.Log("TestPurgeStaleRecords started")
	_, _, err := RS.PersistIntent(ctx, cpu)
	assert.NoError(err)

	cpu2 := &ProtobufWithMetadata{new(pb.CPU), protostore.Metadata{}}
	err = RS.FindById(ctx, id, cpu2)
	assert.NoError(err)
	cpu.Metadata = cpu2.Metadata

	overallStatus, enforcementStatusMap, err := RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: PENDING, actual: overallStatus},
		{expected: UNKNOWN, actual: enforcementStatusMap["e1"]},
		{expected: UNKNOWN, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsInProgress(ctx, "e1", cpu)
	assert.NoError(err)
	err = RS.MarkEnforcementAsInProgress(ctx, "e2", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: IN_PROGRESS, actual: overallStatus},
		{expected: IN_PROGRESS, actual: enforcementStatusMap["e1"]},
		{expected: IN_PROGRESS, actual: enforcementStatusMap["e2"]},
	})

	_, err = RS.SoftDelete(ctx, cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: DELETION_PENDING, actual: overallStatus},
		{expected: DELETION_PENDING, actual: enforcementStatusMap["e1"]},
		{expected: DELETION_PENDING, actual: enforcementStatusMap["e2"]},
	})

	_, err = RS.PurgeStaleStatusRecords(ctx, cpu)
	assert.NoError(err)
	_, _, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.ErrorIs(err, errors.ErrRecordNotFound)

	_, err = RS.Delete(ctx, cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: DELETION_REALIZED, actual: overallStatus},
		{expected: UNKNOWN, actual: enforcementStatusMap["e1"]},
		{expected: UNKNOWN, actual: enforcementStatusMap["e2"]},
	})

	_, err = RS.PurgeStaleStatusRecords(ctx, cpu)
	assert.NoError(err)
	_, _, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.ErrorIs(err, errors.ErrRecordNotFound)

	t.Log("TestPurgeStaleRecords completed")
}

func TestPurgeAllRecords(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()
	id := cpu.Id

	t.Log("TestPurgeAllRecords started")
	_, _, err := RS.PersistIntent(ctx, cpu)
	assert.NoError(err)

	cpu2 := &ProtobufWithMetadata{new(pb.CPU), protostore.Metadata{}}
	err = RS.FindById(ctx, id, cpu2)
	assert.NoError(err)
	cpu.Metadata = cpu2.Metadata

	overallStatus, enforcementStatusMap, err := RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: PENDING, actual: overallStatus},
		{expected: UNKNOWN, actual: enforcementStatusMap["e1"]},
		{expected: UNKNOWN, actual: enforcementStatusMap["e2"]},
	})

	err = RS.MarkEnforcementAsPending(ctx, "e0", cpu)
	assert.NoError(err)
	err = RS.MarkEnforcementAsInProgress(ctx, "e1", cpu)
	assert.NoError(err)
	err = RS.MarkEnforcementAsError(ctx, "e2", "realization failed", cpu)
	assert.NoError(err)
	overallStatus, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatuses(t, []StatusPair{
		{expected: ERROR, actual: overallStatus},
		{expected: PENDING, actual: enforcementStatusMap["e0"]},
		{expected: IN_PROGRESS, actual: enforcementStatusMap["e1"]},
		{expected: ERROR, actual: enforcementStatusMap["e2"]},
	})

	_, err = RS.Purge(ctx, cpu)
	assert.NoError(err)
	_, _, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.ErrorIs(err, errors.ErrRecordNotFound)

	t.Log("TestPurgeAllRecords completed")
}

func rollbackTx(t *testing.T, tx *gorm.DB) {
	t.Helper()
	if err := tx.Rollback().Error; err != nil && err != sql.ErrTxDone {
		t.Log("Rollback of tx errored", err)
	}
}

func TestMarkEnforcementAsSuccess(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()
	id := cpu.Id

	tx, err := DS.GetTransaction(ctx, cpu)
	assert.NoError(err)
	defer rollbackTx(t, tx)
	txFetcher := authorizer.SimpleTransactionFetcher{}
	ctx = txFetcher.WithTransactionCtx(ctx, tx)

	// UPSERT ACTING AS INSERTION
	if _, _, err := RS.PersistIntent(ctx, cpu); err != nil {
		assert.FailNow("Failed to insert a resource", err)
	}

	cpu = &ProtobufWithMetadata{new(pb.CPU), protostore.Metadata{}}
	if err := RS.FindById(ctx, id, cpu); err != nil {
		assert.FailNow("Failed to find resource by ID", err)
	}

	// Mark enforcement as pending
	enforcementPoints := [3]string{"cluster-001", "cluster-002", "cluster-003"}
	for _, enforcementPoint := range enforcementPoints {
		if err := RS.MarkEnforcementAsPending(ctx, enforcementPoint, cpu); err != nil {
			assert.FailNow("Failed to mark enforcement as pending", err)
		}
	}

	// Mark enforcement as success
	for i, enforcementPoint := range enforcementPoints {
		err := RS.MarkEnforcementAsSuccess(ctx, enforcementPoint, cpu)
		assert.NoError(err, "Failed to mark enforcement as success")

		overallStatus, enforcementStatusMap, err := RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
		assert.NoError(err)
		assert.Contains(enforcementStatusMap, enforcementPoint)
		checkStatus(t, REALIZED, enforcementStatusMap[enforcementPoint])

		switch i {
		case 0, 1:
			checkStatus(t, overallStatus, PENDING)
		case 2: // After enforcement has been marked as success on the last enforcement point, overall status should also be success (realized)
			checkStatus(t, overallStatus, REALIZED)
		}
	}
	testResourceRevisionConsistency(t, ctx, id, &pb.CPU{}, enforcementPoints[:]...)
	assert.NoError(tx.Commit().Error)
}

func TestMarkEnforcementAsError(t *testing.T) {
	assert := assert.New(t)
	ctx := setupRealizationStore(t, COKE, TENANT_ADMIN)

	cpu := getSampleCPU()
	id := cpu.Id

	// UPSERT ACTING AS INSERTION
	if _, _, err := RS.PersistIntent(ctx, cpu); err != nil {
		assert.FailNow("Failed to insert a resource", err)
	}

	cpu = &ProtobufWithMetadata{new(pb.CPU), protostore.Metadata{}}
	if err := RS.FindById(ctx, id, cpu); err != nil {
		assert.FailNow("Failed to find resource by ID", err)
	}

	// Mark enforcement as pending
	enforcementPoints := [3]string{"cluster-001", "cluster-002", "cluster-003"}
	for _, enforcementPoint := range enforcementPoints {
		if err := RS.MarkEnforcementAsPending(ctx, enforcementPoint, cpu); err != nil {
			assert.FailNow("Failed to mark enforcement as pending", err)
		}
	}

	// Mark enforcement as success
	for _, enforcementPoint := range enforcementPoints {
		if err := RS.MarkEnforcementAsSuccess(ctx, enforcementPoint, cpu); err != nil {
			assert.FailNow("Failed to get mark enforcement as success", err)
		}

		if _, _, err := RS.GetOverallStatusWithEnforcementDetails(ctx, cpu); err != nil {
			assert.FailNow("Failed to get overall status with enforcement details", err)
		}
	}

	// Mark one of the enforcements as failure
	errMsg1 := strings.Repeat("A", ADDITIONAL_DETAILS_LENGTH_CAP)
	err := RS.MarkEnforcementAsError(ctx, enforcementPoints[0], errMsg1, cpu)
	assert.NoError(err)

	overallStatus, enforcementStatusMap, err := RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	checkStatus(t, ERROR, overallStatus, "Expected overall status to be an error if enforcement failed at any enforcement point")
	assert.Contains(enforcementStatusMap, enforcementPoints[0])
	checkStatus(t, ERROR, enforcementStatusMap[enforcementPoints[0]], "Expected enforcement status at %s to be ERROR", enforcementPoints[0])

	errMsg2 := strings.Repeat("B", 100)
	err = RS.MarkEnforcementAsError(ctx, enforcementPoints[0], errMsg2, cpu)
	assert.NoError(err)

	overallStatusRecord, enforcementStatusRecordMap, err := RS.GetEnforcementStatusMap(ctx, cpu)
	assert.NoError(err)
	enforcementStatusRecord := enforcementStatusRecordMap[enforcementPoints[0]]
	assert.LessOrEqualf(len(enforcementStatusRecord.AdditionalDetails), ADDITIONAL_DETAILS_LENGTH_CAP,
		"Expected the length of additional_details to be capped at %d", ADDITIONAL_DETAILS_LENGTH_CAP)
	assert.Truef(strings.Contains(enforcementStatusRecord.AdditionalDetails, errMsg2),
		"Expected the most recent error message of length %d to fit into additional_details", len(errMsg2))
	assert.Falsef(strings.Contains(enforcementStatusRecord.AdditionalDetails, errMsg1),
		"Expected the older error message of length %d not to fit into additional_details", len(errMsg1))
	assert.Truef(strings.Contains(enforcementStatusRecord.AdditionalDetails, errMsg1[:100]),
		"Expected a truncated version of an older message to still be in additional_details")

	assert.LessOrEqualf(len(overallStatusRecord.AdditionalDetails), ADDITIONAL_DETAILS_LENGTH_CAP,
		"Expected the length of additional_details to be capped at %d", ADDITIONAL_DETAILS_LENGTH_CAP)
	assert.Truef(strings.Contains(overallStatusRecord.AdditionalDetails, errMsg2),
		"Expected the most recent error message of length %d to fit into additional_details", len(errMsg2))
	assert.Falsef(strings.Contains(overallStatusRecord.AdditionalDetails, errMsg1),
		"Expected the older error message of length %d not to fit into additional_details", len(errMsg1))
	assert.Truef(strings.Contains(overallStatusRecord.AdditionalDetails, errMsg1[:100]),
		"Expected a truncated version of an older message to still be in additional_details")

	testResourceRevisionConsistency(t, ctx, id, &pb.CPU{}, enforcementPoints[:]...)
}

func TestMarkEnforcementAsDeletionPending(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()

	// UPSERT ACTING AS INSERTION
	if _, _, err := RS.PersistIntent(ctx, cpu); err != nil {
		assert.FailNow("Failed to insert a record", err)
	}

	if _, err := RS.Delete(ctx, cpu); err != nil {
		assert.FailNow("Failed to delete a record", err)
	}

	const enforcementPoint = "cluster-001"
	err := RS.MarkEnforcementAsDeletionPending(ctx, enforcementPoint, cpu)
	assert.NoError(err)

	_, enforcementStatusMap, err := RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	assert.Contains(enforcementStatusMap, enforcementPoint)
	checkStatus(t, DELETION_PENDING, enforcementStatusMap[enforcementPoint])

	err = RS.MarkEnforcementAsDeletionInProgress(ctx, enforcementPoint, cpu)
	assert.NoError(err)

	_, enforcementStatusMap, err = RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
	assert.NoError(err)
	assert.Contains(enforcementStatusMap, enforcementPoint)
	checkStatus(t, DELETION_IN_PROGRESS, enforcementStatusMap[enforcementPoint])
}

func TestMarkEnforcementAsDeletionOfExistingResource(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()

	// UPSERT ACTING AS INSERTION
	if _, _, err := RS.PersistIntent(ctx, cpu); err != nil {
		assert.FailNow("Failed to insert a record", err)
	}

	err := RS.MarkEnforcementAsDeletionPending(ctx, "cluster-001", cpu)
	assert.NoError(err) // Enforcement Span change case

	err = RS.MarkEnforcementAsDeletionRealized(ctx, "cluster-001", cpu)
	assert.NoError(err) // Enforcement Span change case
}

func TestMarkEnforcementAsDeletionRealized(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()

	// UPSERT ACTING AS INSERTION
	if _, _, err := RS.PersistIntent(ctx, cpu); err != nil {
		assert.FailNow("Failed to insert a record", err)
	}

	if _, err := RS.Delete(ctx, cpu); err != nil {
		assert.FailNow("Failed to delete a record", err)
	}

	// Mark enforcement as deletion realized
	enforcementPoints := [3]string{"cluster-001", "cluster-002", "cluster-003"}
	for i, enforcementPoint := range enforcementPoints {
		err := RS.MarkEnforcementAsDeletionRealized(ctx, enforcementPoint, cpu)
		assert.NoError(err)

		overallStatus, enforcementStatusMap, err := RS.GetOverallStatusWithEnforcementDetails(ctx, cpu)
		assert.NoError(err)
		assert.Contains(enforcementStatusMap, enforcementPoint)
		checkStatus(t, DELETION_REALIZED, enforcementStatusMap[enforcementPoint])

		// After resource has been "unenforced" from all enforcement points, overall status should be set to deletion realized
		if i == 2 {
			checkStatus(t, overallStatus, DELETION_REALIZED)
		}
	}
}

/*
Rule update with enforcement point changes: rule1(point1, point2) -> rule1(point2, point3),
For a rule update, point amount could either increase or decrease. if decreased, some of the enforcement
points should be in deletion_pending, but for the overall status it is still pending, which means the overall
transaction should be treated as update.
*/
func TestEnforcementAmountChangeInTransaction(t *testing.T) {
	assert := assert.New(t)
	const orgId = PEPSI
	ctx := setupRealizationStore(t, orgId, TENANT_ADMIN)

	cpu := getSampleCPU()

	// UPSERT ACTING AS INSERTION
	if _, _, err := RS.PersistIntent(ctx, cpu); err != nil {
		assert.FailNow("Failed to insert a record", err)
	}

	err := RS.MarkEnforcementAsPending(ctx, "cluster-001", cpu)
	assert.NoError(err)
	err = RS.MarkEnforcementAsPending(ctx, "cluster-002", cpu)
	assert.NoError(err)

	err = RS.MarkEnforcementAsSuccess(ctx, "cluster-001", cpu)
	assert.NoError(err)
	err = RS.MarkEnforcementAsSuccess(ctx, "cluster-002", cpu)
	assert.NoError(err)

	err = RS.MarkEnforcementAsDeletionPending(ctx, "cluster-001", cpu)
	assert.NoError(err)
	err = RS.MarkEnforcementAsPending(ctx, "cluster-002", cpu)
	assert.NoError(err)
	err = RS.MarkEnforcementAsPending(ctx, "cluster-003", cpu)
	assert.NoError(err)

	status, err := RS.GetOverallStatus(ctx, cpu)
	assert.NoError(err)
	checkStatus(t, PENDING, status)

	err = RS.MarkEnforcementAsDeletionRealized(ctx, "cluster-001", cpu)
	assert.NoError(err)
	err = RS.MarkEnforcementAsSuccess(ctx, "cluster-002", cpu)
	assert.NoError(err)
	err = RS.MarkEnforcementAsSuccess(ctx, "cluster-003", cpu)
	assert.NoError(err)

	status, err = RS.GetOverallStatus(ctx, cpu)
	assert.NoError(err)
	checkStatus(t, REALIZED, status)
}

/*
Checks that the resource with the given ID has the same revision as its overall status and enforcement statuses records have.
Can be used at the enf of a unit test to confirm that DB is still in a consistent state.
*/
func testResourceRevisionConsistency(
	t *testing.T,
	ctx context.Context,
	id string,
	msg proto.Message, // Can be an empty Protobuf message; used to determine concrete type of Protobuf struct
	enforcementPoints ...string,
) {
	t.Helper()
	assert := assert.New(t)

	queryResults := &ProtobufWithMetadata{
		Message:  msg,
		Metadata: protostore.Metadata{Id: id},
	}
	err := RS.FindById(ctx, id, queryResults)
	assert.NoError(err)

	overallStatusRecord, enforcementStatusRecordMap, err := RS.GetEnforcementStatusMap(ctx, queryResults)
	assert.NoError(err)
	assert.Equal(queryResults.Revision, overallStatusRecord.Revision)
	assert.Len(enforcementStatusRecordMap, len(enforcementPoints))
	for _, enforcementPoint := range enforcementPoints {
		assert.Contains(enforcementStatusRecordMap, enforcementPoint)
		assert.Equal(queryResults.Revision, enforcementStatusRecordMap[enforcementPoint].Revision)
	}
}
