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

package protostore_test

import (
	"context"
	"fmt"
	"io"
	"sort"
	"testing"

	"github.com/bxcodec/faker/v3"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/protostore"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/test"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/test/pb"
)

type MemorySlice []pb.Memory

func (s MemorySlice) Len() int {
	return len(s)
}

func (s MemorySlice) Less(i, j int) bool {
	return s[i].String() < s[j].String()
}

func (s MemorySlice) Swap(i, j int) {
	s[i].Brand, s[j].Brand = s[j].Brand, s[i].Brand
	s[i].Size, s[j].Size = s[j].Size, s[i].Size
	s[i].Speed, s[j].Speed = s[j].Speed, s[i].Speed
	s[i].Type, s[j].Type = s[j].Type, s[i].Type
}

type MemoryPtrSlice []*pb.Memory

func (s MemoryPtrSlice) Len() int {
	return len(s)
}

func (s MemoryPtrSlice) Less(i, j int) bool {
	return s[i].String() < s[j].String()
}

func (s MemoryPtrSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type CPUSlice []pb.CPU

func (s CPUSlice) Len() int {
	return len(s)
}

func (s CPUSlice) Less(i, j int) bool {
	return s[i].String() < s[j].String()
}

func (s CPUSlice) Swap(i, j int) {
	s[i].Brand, s[j].Brand = s[j].Brand, s[i].Brand
	s[i].Name, s[j].Name = s[j].Name, s[i].Name
	s[i].NumberCores, s[j].NumberCores = s[j].NumberCores, s[i].NumberCores
	s[i].NumberThreads, s[j].NumberThreads = s[j].NumberThreads, s[i].NumberThreads
	s[i].MinGhz, s[j].MinGhz = s[j].MinGhz, s[i].MinGhz
	s[i].MaxGhz, s[j].MaxGhz = s[j].MaxGhz, s[i].MaxGhz
}

type CPUPtrSlice []*pb.CPU

func (s CPUPtrSlice) Len() int {
	return len(s)
}

func (s CPUPtrSlice) Less(i, j int) bool {
	return s[i].String() < s[j].String()
}

func (s CPUPtrSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

const (
	P1 = "P1"
	P2 = "P2"
	P4 = "P4"
)

func TestProtoConversionWithFaker(t *testing.T) {
	assert := assert.New(t)
	msg1 := pb.CPU{}
	_ = faker.FakeData(&msg1)
	data, _ := protostore.ToBytes(&msg1)
	msg2 := pb.CPU{}
	_ = protostore.FromBytes(data, &msg2)

	assert.Equal(msg1.String(), msg2.String())
}

func TestProtoConversion(t *testing.T) {
	assert := assert.New(t)
	msg1 := pb.CPU{
		Brand:         "Intel",
		Name:          "Pentium",
		NumberCores:   4,
		NumberThreads: 8,
		MinGhz:        1.6,
	}
	data, _ := protostore.ToBytes(&msg1)
	msg2 := pb.CPU{}
	_ = protostore.FromBytes(data, &msg2)

	assert.Equal(msg1.String(), msg2.String())

	msg3 := pb.Memory{
		Brand: "Samsung",
		Size:  32,
		Speed: 2933,
		Type:  "DDR4",
	}
	data, _ = protostore.ToBytes(&msg3)
	msg4 := pb.Memory{}
	_ = protostore.FromBytes(data, &msg4)

	assert.Equal(msg3.String(), msg4.String())
}

func setupDbContext(t *testing.T, dbName string) protostore.ProtoStore {
	t.Helper()
	assert := assert.New(t)
	_, p := SetupDataStore(dbName)
	protoMsgs := []proto.Message{
		&pb.CPU{},
		&pb.Memory{},
	}

	if err := p.DropTables(protoMsgs...); err != nil {
		assert.FailNow("Dropping tables failed with error: " + err.Error())
	}

	roleMapping := map[string]dbrole.DbRole{
		TENANT_AUDITOR:  dbrole.TENANT_READER,
		TENANT_ADMIN:    dbrole.TENANT_WRITER,
		SERVICE_AUDITOR: dbrole.READER,
		SERVICE_ADMIN:   dbrole.WRITER,
	}
	err := p.Register(context.TODO(), roleMapping, protoMsgs...)
	assert.NoError(err)
	return p
}

// setupDbContext creates ProtoStore that uses MetadataAuthorizer and SimpleInstancer.
func setupDbContextWithInstancer(t *testing.T, dbName string, dropTables bool) protostore.ProtoStore {
	t.Helper()
	assert := assert.New(t)
	_, p := SetupDataStore(dbName)
	protoMsgs := []proto.Message{
		&pb.CPU{},
		&pb.Memory{},
	}

	if dropTables {
		if err := p.DropTables(protoMsgs...); err != nil {
			assert.FailNow("Dropping tables failed with error: " + err.Error())
		}
	}

	roleMapping := map[string]dbrole.DbRole{
		TENANT_AUDITOR:  dbrole.TENANT_READER,
		TENANT_ADMIN:    dbrole.TENANT_WRITER,
		SERVICE_AUDITOR: dbrole.READER,
		SERVICE_ADMIN:   dbrole.WRITER,
	}
	err := p.Register(context.TODO(), roleMapping, protoMsgs...)
	assert.NoError(err)
	return p
}

// setupDbContext creates ProtoStore that uses MetadataAuthorizer.
func setupDbContextNoInstancer(t *testing.T, dbName string, dropTables bool) protostore.ProtoStore {
	t.Helper()
	assert := assert.New(t)
	_, p := SetupDataStoreNoInstancer(dbName)
	protoMsgs := []proto.Message{
		&pb.CPU{},
		&pb.Memory{},
	}

	if dropTables {
		if err := p.DropTables(protoMsgs...); err != nil {
			assert.FailNow("Dropping tables failed with error: " + err.Error())
		}
	}

	roleMapping := map[string]dbrole.DbRole{
		TENANT_AUDITOR:  dbrole.TENANT_READER,
		TENANT_ADMIN:    dbrole.TENANT_WRITER,
		SERVICE_AUDITOR: dbrole.READER,
		SERVICE_ADMIN:   dbrole.WRITER,
	}
	err := p.Register(context.TODO(), roleMapping, protoMsgs...)
	assert.NoError(err)
	return p
}

func TestProtoStoreInDbFindWithInvalidParams(t *testing.T) {
	p := setupDbContext(t, "TestProtoStoreInDbFindWithInvalidParams")
	testProtoStoreFindWithInvalidParams(t, p, AmericasPepsiAdminCtx)
}

/*
Checks that ProtobufDataStore.FindAll and ProtobufDataStore.FindAllAsMap reject requests containing invalid arguments
where query results are supposed to be stored and do not reject those that contain valid arguments.
*/
func testProtoStoreFindWithInvalidParams(t *testing.T, p protostore.ProtoStore, ctx context.Context) {
	t.Helper()
	assert := assert.New(t)

	// FIND ALL
	{
		invalidParams := []interface{}{
			pb.CPU{},
			&pb.CPU{},
			[]pb.CPU{},
			[]*pb.CPU{},
			map[string]*pb.CPU{},
		}

		for _, invalidParam := range invalidParams {
			t.Logf("Testing with invalidParam %s=%+v", datastore.TypeName(invalidParam), invalidParam)
			_, err := p.FindAll(ctx, invalidParam, datastore.NoPagination())
			assert.ErrorIs(err, ErrNotPtrToStructSlice)
		}

		validParams := []interface{}{
			&[]pb.CPU{},
			&[]*pb.CPU{},
		}

		for _, validParam := range validParams {
			t.Logf("Testing with validParam %s=%+v", datastore.TypeName(validParam), validParam)
			_, err := p.FindAll(ctx, validParam, datastore.NoPagination())
			assert.NoError(err)
		}
	}

	// FIND ALL AS MAP
	{
		invalidParams := []interface{}{
			pb.CPU{},
			&pb.CPU{},
			[]pb.CPU{},
			[]*pb.CPU{},
			&(map[string]*pb.CPU{}),
		}

		for i := range invalidParams {
			invalidParam := invalidParams[i]
			_, err := p.FindAllAsMap(ctx, invalidParam, datastore.NoPagination())
			assert.ErrorIs(err, ErrNotPtrToStructSlice)
		}

		validParams := []interface{}{
			map[string]*pb.CPU{},
			map[string]pb.CPU{},
		}

		for i := range validParams {
			validParam := validParams[i]
			_, err := p.FindAllAsMap(ctx, validParam, datastore.NoPagination())
			assert.NoError(err)
		}
	}
}

/*
TestProtoStoreInDbFindAll_DifferentProducerConsumer_InitProducerFirst checks if the "consumer" (who uses ProtoStore instance
with instancer DISABLED) is able to read data written to the DB by the "producer" (who uses ProtoStore instance with
instancer ENABLED). The producer will insert Protobuf messages into the DB and fill out instance_id column of the table.

The test case will first initialize "producer's" ProtoStore instance and later "consumer's" one.
*/
func TestProtoStoreInDbFindAll_DifferentProducerConsumer_InitProducerFirst(t *testing.T) {
	const dbName = "TestProtoStoreInDbFindAll_DifferentProducerConsumer"
	t.Log("Creating producer's ProtoStore instance (instancer ENABLED)")
	pNoInstancer := setupDbContextNoInstancer(t, dbName, true /* dropTables */)

	t.Log("Creating consumer's ProtoStore instance (instancer DISABLED)")
	pWithInstancer := setupDbContextWithInstancer(t, dbName, false /* dropTables */)

	testProtoStoreInDbFindAll_DifferentProducerConsumer(t, pNoInstancer, pWithInstancer)
}

// Same as TestProtoStoreInDbFindAll_DifferentProducerConsumer_InitProducerFirst, but "consumer's" ProtoStore instance
// is initialized first.
func TestProtoStoreInDbFindAll_DifferentProducerConsumer_InitConsumerFirst(t *testing.T) {
	const dbName = "TestProtoStoreInDbFindAll_DifferentProducerConsumer"
	t.Log("Creating consumer's ProtoStore instance (instancer DISABLED)")
	pWithInstancer := setupDbContextWithInstancer(t, dbName, true /* dropTables */)

	t.Log("Creating producer's ProtoStore instance (instancer ENABLED)")
	pNoInstancer := setupDbContextNoInstancer(t, dbName, false /* dropTables */)

	testProtoStoreInDbFindAll_DifferentProducerConsumer(t, pNoInstancer, pWithInstancer)
}

// Same as TestProtoStoreInDbFindAll, but different ProtoStore instances are used to read and write data
// (ProtoStore instance that writes data has instancer enabled; ProtoStore instance that reads data does not have instancer enabled).

// testProtoStoreInDbFindAll_DifferentProducerConsumer checks if a ProtoStore instance that has no instancer configured is able to
// read the data from the DB that was written to the DB by a ProtoStore instance that did have instancer enabled.
func testProtoStoreInDbFindAll_DifferentProducerConsumer(t *testing.T, pNoInstancer, pWithInstancer protostore.ProtoStore) {
	t.Helper()
	assert := assert.New(t)

	memMsg1, memMsg2 := pb.Memory{}, pb.Memory{}
	for _, protoMsg := range []proto.Message{&memMsg1, &memMsg2} {
		_ = faker.FakeData(protoMsg)
	}

	t.Log("Inserting Pepsi's data into DB under Americas instance")
	rowsAffected, md, err := pWithInstancer.Insert(AmericasPepsiAdminCtx, P1, &memMsg1)
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(P1, md.Id)
	assert.Equal(int64(1), md.Revision)

	rowsAffected, md, err = pWithInstancer.Insert(AmericasPepsiAdminCtx, P2, &memMsg2)
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(P2, md.Id)
	assert.Equal(int64(1), md.Revision)

	t.Log("Checking if Pepsi's data is visible to Pepsi admin (no instancer configured)")
	var expectedMemoryQueryResults MemoryPtrSlice = []*pb.Memory{&memMsg1, &memMsg2}
	sort.Sort(expectedMemoryQueryResults)
	var actualQueryResults MemorySlice = make([]pb.Memory, 0)

	metadataMap, err := pNoInstancer.FindAll(PepsiAdminCtx, &actualQueryResults, datastore.NoPagination())
	assert.NoError(err)
	assert.Len(actualQueryResults, 2)
	assert.Len(metadataMap, 2)

	sort.Sort(actualQueryResults)
	assert.Equal(expectedMemoryQueryResults[0].String(), actualQueryResults[0].String())
	assert.Equal(expectedMemoryQueryResults[1].String(), actualQueryResults[1].String())
}

/*
Checks if DIFFERENT types of Protobuf messages can be persisted with the same IDs.
Checks if FindAll() works (both when query results are stored in a ptr to a slice of structs and
when query results are stored in a ptr to a slice of ptrs to structs ).
*/
func TestProtoStoreInDbFindAll(t *testing.T) {
	assert := assert.New(t)
	p := setupDbContext(t, "TestProtoStoreInDbFindAll")
	ctx := AmericasPepsiAdminCtx

	// Prepare data for test cases
	memMsg1, memMsg2, cpuMsg1, cpuMsg2 := pb.Memory{}, pb.Memory{}, pb.CPU{}, pb.CPU{}
	for _, protoMsg := range []proto.Message{&memMsg1, &memMsg2, &cpuMsg1, &cpuMsg2} {
		_ = faker.FakeData(protoMsg)
	}
	rowsAffected, md, err := p.Insert(ctx, P1, &memMsg1)
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(P1, md.Id)
	assert.Equal(int64(1), md.Revision)
	assert.Equal(AMERICAS, md.InstanceId)

	rowsAffected, md, err = p.Insert(ctx, P2, &memMsg2)
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(P2, md.Id)
	assert.Equal(int64(1), md.Revision)
	assert.Equal(AMERICAS, md.InstanceId)

	rowsAffected, md, err = p.Insert(ctx, P1, &cpuMsg1)
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(P1, md.Id)
	assert.Equal(int64(1), md.Revision)
	assert.Equal(AMERICAS, md.InstanceId)

	rowsAffected, md, err = p.Insert(ctx, P2, &cpuMsg2)
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(P2, md.Id)
	assert.Equal(int64(1), md.Revision)
	assert.Equal(AMERICAS, md.InstanceId)

	var expectedMemoryQueryResults MemoryPtrSlice = []*pb.Memory{&memMsg1, &memMsg2}
	var expectedCPUQueryResults CPUPtrSlice = []*pb.CPU{&cpuMsg1, &cpuMsg2}
	sort.Sort(expectedMemoryQueryResults)
	sort.Sort(expectedCPUQueryResults)

	{
		// Check if you can find all memory records when passing a pointer to slice of structs
		var actualQueryResults MemorySlice = make([]pb.Memory, 0)
		metadataMap, err := p.FindAll(ctx, &actualQueryResults, datastore.NoPagination())
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		sort.Sort(actualQueryResults)
		assert.Equal(expectedMemoryQueryResults[0].String(), actualQueryResults[0].String())
		assert.Equal(expectedMemoryQueryResults[1].String(), actualQueryResults[1].String())
	}

	{
		// Check if you can find all memory records when passing a pointer to slice of pointers to structs
		var actualQueryResults MemoryPtrSlice = make([]*pb.Memory, 0)
		metadataMap, err := p.FindAll(ctx, &actualQueryResults, datastore.NoPagination())
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		sort.Sort(actualQueryResults)
		assert.Equal(expectedMemoryQueryResults[0].String(), actualQueryResults[0].String())
		assert.Equal(expectedMemoryQueryResults[1].String(), actualQueryResults[1].String())

		actualQueryResults = make([]*pb.Memory, 0)
		page1 := make([]*pb.Memory, 0)
		page2 := make([]*pb.Memory, 0)
		metadataMap, err = p.FindAll(ctx, &page1, datastore.GetPagination(0, 1, ""))
		assert.NoError(err)
		assert.Len(page1, 1)
		assert.Len(metadataMap, 1)
		actualQueryResults = append(actualQueryResults, page1[0])
		metadataMap, err = p.FindAll(ctx, &page2, datastore.GetPagination(1, 1, ""))
		assert.NoError(err)
		assert.Len(page2, 1)
		assert.Len(metadataMap, 1)
		actualQueryResults = append(actualQueryResults, page2[0])

		sort.Sort(actualQueryResults)
		assert.Equal(expectedMemoryQueryResults[0].String(), actualQueryResults[0].String())
		assert.Equal(expectedMemoryQueryResults[1].String(), actualQueryResults[1].String())
	}

	{
		// Check if you can find all memory records when passing a pointer to map of strings to structs
		actualQueryResults := make(map[string]pb.Memory)
		metadataMap, err := p.FindAllAsMap(ctx, actualQueryResults, datastore.NoPagination())
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		expectedQueryResults := make(map[string]pb.Memory)
		expectedQueryResults[P1] = pb.Memory{
			Brand: memMsg1.Brand,
			Size:  memMsg1.Size,
			Speed: memMsg1.Speed,
			Type:  memMsg1.Type,
		}
		expectedQueryResults[P2] = pb.Memory{
			Brand: memMsg2.Brand,
			Size:  memMsg2.Size,
			Speed: memMsg2.Speed,
			Type:  memMsg2.Type,
		}

		for _, id := range []string{P1, P2} {
			expectedQueryResult := pb.Memory{
				Brand: expectedQueryResults[id].Brand,
				Size:  expectedQueryResults[id].Size,
				Speed: expectedQueryResults[id].Speed,
				Type:  expectedQueryResults[id].Type,
			}
			actualQueryResult := pb.Memory{
				Brand: actualQueryResults[id].Brand,
				Size:  actualQueryResults[id].Size,
				Speed: actualQueryResults[id].Speed,
				Type:  actualQueryResults[id].Type,
			}
			assert.Equal(expectedQueryResult.String(), actualQueryResult.String())
		}
	}

	{
		// Check if you can find all memory records when passing a pointer to map of strings to pointers to structs
		actualQueryResults := make(map[string]*pb.Memory)
		metadataMap, err := p.FindAllAsMap(ctx, actualQueryResults, datastore.NoPagination())
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		expectedQueryResults := make(map[string]*pb.Memory)
		expectedQueryResults[P1] = &memMsg1
		expectedQueryResults[P2] = &memMsg2

		for _, id := range []string{P1, P2} {
			expectedQueryResult, actualQueryResult := expectedQueryResults[id], actualQueryResults[id]
			assert.Equal(expectedQueryResult.String(), actualQueryResult.String())
		}
	}

	{
		// Check if you can find all CPU records when passing a pointer to slice of structs
		var actualQueryResults CPUSlice = make([]pb.CPU, 0)
		metadataMap, err := p.FindAll(ctx, &actualQueryResults, datastore.NoPagination())
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		sort.Sort(actualQueryResults)
		assert.Equal(expectedCPUQueryResults[0].String(), actualQueryResults[0].String())
		assert.Equal(expectedCPUQueryResults[1].String(), actualQueryResults[1].String())
	}

	{
		// Check if you can find all CPU records when passing a pointer to slice of pointers to structs
		var actualQueryResults CPUPtrSlice = make([]*pb.CPU, 0)
		metadataMap, err := p.FindAll(ctx, &actualQueryResults, datastore.NoPagination())
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		sort.Sort(actualQueryResults)
		assert.Equal(expectedCPUQueryResults[0].String(), actualQueryResults[0].String())
		assert.Equal(expectedCPUQueryResults[1].String(), actualQueryResults[1].String())
	}

	{
		// Check if you can find all CPU records when passing a pointer to map of strings to structs
		actualQueryResults := make(map[string]pb.CPU)
		metadataMap, err := p.FindAllAsMap(ctx, actualQueryResults, datastore.NoPagination())
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		expectedQueryResults := make(map[string]pb.CPU)
		expectedQueryResults[P1] = pb.CPU{
			Brand:         cpuMsg1.Brand,
			Name:          cpuMsg1.Name,
			NumberCores:   cpuMsg1.NumberCores,
			NumberThreads: cpuMsg1.NumberThreads,
			MinGhz:        cpuMsg1.MinGhz,
			MaxGhz:        cpuMsg1.MaxGhz,
		}
		expectedQueryResults[P2] = pb.CPU{
			Brand:         cpuMsg2.Brand,
			Name:          cpuMsg2.Name,
			NumberCores:   cpuMsg2.NumberCores,
			NumberThreads: cpuMsg2.NumberThreads,
			MinGhz:        cpuMsg2.MinGhz,
			MaxGhz:        cpuMsg2.MaxGhz,
		}

		for _, id := range []string{P1, P2} {
			expectedQueryResult := pb.CPU{
				Brand:         expectedQueryResults[id].Brand,
				Name:          expectedQueryResults[id].Name,
				NumberCores:   expectedQueryResults[id].NumberCores,
				NumberThreads: expectedQueryResults[id].NumberThreads,
				MinGhz:        expectedQueryResults[id].MinGhz,
				MaxGhz:        expectedQueryResults[id].MaxGhz,
			}
			actualQueryResult := pb.CPU{
				Brand:         actualQueryResults[id].Brand,
				Name:          actualQueryResults[id].Name,
				NumberCores:   actualQueryResults[id].NumberCores,
				NumberThreads: actualQueryResults[id].NumberThreads,
				MinGhz:        actualQueryResults[id].MinGhz,
				MaxGhz:        actualQueryResults[id].MaxGhz,
			}
			assert.Equal(expectedQueryResult.String(), actualQueryResult.String())
		}
	}

	{
		// Check if you can find all CPU records when passing a pointer to map of strings to pointers to structs
		actualQueryResults := make(map[string]*pb.CPU)
		metadataMap, err := p.FindAllAsMap(ctx, actualQueryResults, datastore.NoPagination())
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		expectedQueryResults := make(map[string]*pb.CPU)
		expectedQueryResults[P1] = &cpuMsg1
		expectedQueryResults[P2] = &cpuMsg2

		for _, id := range []string{P1, P2} {
			expectedQueryResult, actualQueryResult := expectedQueryResults[id], actualQueryResults[id]
			assert.Equal(expectedQueryResult.String(), actualQueryResult.String())
		}
	}
	{
		protoMsgs := []proto.Message{
			&pb.CPU{},
			&pb.Memory{},
		}

		err := p.DropTables(protoMsgs...)
		assert.NoError(err)

		cpuQueryResults := make(map[string]*pb.CPU)
		_, err = p.FindAllAsMap(ctx, cpuQueryResults, datastore.NoPagination())
		assert.ErrorIs(err, ErrExecutingSqlStmt)

		memoryQueryResults := make([]*pb.Memory, 0)
		_, err = p.FindAll(ctx, &memoryQueryResults, datastore.NoPagination())
		assert.ErrorIs(err, ErrExecutingSqlStmt)
	}
}

func TestProtoStoreInDbCrud(t *testing.T) {
	p := setupDbContext(t, "TestProtoStoreInDbCrud")
	testProtoStoreCrud(t, p, AmericasPepsiAdminCtx, false)
}

// Same as TestProtoStoreInDbCrud, but uses Upsert instead of Insert or Update.
func TestProtoStoreInDbCrudUpsert(t *testing.T) {
	p := setupDbContext(t, "TestProtoStoreInDbCrudUpsert")
	testProtoStoreCrud(t, p, AmericasPepsiAdminCtx, true)
}

/*
Performs CRUD-operations with ProtoStore.
If useUpsert is true, will use Upsert instead of Insert and Update.
Otherwise, Insert and Update will be used.
*/
func testProtoStoreCrud(t *testing.T, p protostore.ProtoStore, ctx context.Context, useUpsert bool) {
	t.Helper()
	assert := assert.New(t)
	var err error
	var rowsAffected int64
	var metadata1, metadata2, metadata3, metadata4, metadata5 protostore.Metadata

	// Insert Protobuf record
	cpuMsg1 := pb.CPU{}
	_ = faker.FakeData(&cpuMsg1)
	if useUpsert {
		rowsAffected, _, err = p.UpsertWithMetadata(ctx, P4, &cpuMsg1, protostore.Metadata{Revision: 1})
	} else {
		rowsAffected, _, err = p.Insert(ctx, P4, &cpuMsg1)
	}
	assert.NoError(err, "Failed to insert a Protobuf message into ProtoStore")
	assert.Equal(int64(1), rowsAffected)

	// Query Protobuf record
	cpuMsg2 := pb.CPU{}
	err = p.FindById(ctx, P4, &cpuMsg2, &metadata1)
	assert.NoError(err, "Failed to find a Protobuf message in ProtoStore")
	assert.Equal(cpuMsg1.String(), cpuMsg2.String())
	revision, err := p.GetRevision(ctx, P4, &pb.CPU{})
	assert.NoError(err)
	assert.Equal(int64(1), revision)

	// Update Protobuf record (with metadata provided explicitly)
	cpuMsg3 := pb.CPU{}
	_ = faker.FakeData(&cpuMsg3)
	if useUpsert {
		rowsAffected, metadata2, err = p.UpsertWithMetadata(ctx, P4, &cpuMsg3, metadata1)
	} else {
		rowsAffected, metadata2, err = p.UpdateWithMetadata(ctx, P4, &cpuMsg3, metadata1)
	}
	assert.NoError(err, "Failed to update a Protobuf message in ProtoStore")
	assert.EqualValues(1, rowsAffected)
	assert.EqualValues(metadata1.Revision+1, metadata2.Revision, "Revision did not increment by 1 after an update")
	revision, err = p.GetRevision(ctx, P4, &pb.CPU{})
	assert.NoError(err)
	assert.Equal(metadata1.Revision+1, revision)

	// Query updated Protobuf record
	cpuMsg4 := pb.CPU{}
	err = p.FindById(ctx, P4, &cpuMsg4, &metadata2)
	assert.NoError(err, "Failed to find the updated Protobuf message in ProtoStore")
	assert.Equal(cpuMsg3.String(), cpuMsg4.String())
	assert.EqualValues(metadata1.Revision+1, metadata2.Revision, "Revision did not increment by 1 after an update")
	revision, err = p.GetRevision(ctx, P4, &pb.CPU{})
	assert.NoError(err)
	assert.Equal(metadata1.Revision+1, revision)

	// Update Protobuf record (without metadata)
	cpuMsg5 := pb.CPU{}
	_ = faker.FakeData(&cpuMsg5)
	cpuMsg5String := cpuMsg5.String()
	if useUpsert {
		rowsAffected, metadata3, err = p.Upsert(ctx, P4, &cpuMsg5)
	} else {
		rowsAffected, metadata3, err = p.Update(ctx, P4, &cpuMsg5)
	}
	assert.NoError(err, "Failed to update a Protobuf message in ProtoStore")
	assert.EqualValues(1, rowsAffected)
	assert.NotEqual(cpuMsg4.String(), cpuMsg5.String())
	assert.Equal(cpuMsg5String, cpuMsg5.String())
	assert.EqualValues(metadata2.Revision+1, metadata3.Revision, "Revision did not increment by 1 after an update")
	revision, err = p.GetRevision(ctx, P4, &pb.CPU{})
	assert.NoError(err)
	assert.Equal(metadata2.Revision+1, revision)

	// Query all Protobuf records
	allCpus := make([]pb.CPU, 0)
	metadataMap, err := p.FindAll(ctx, &allCpus, datastore.NoPagination())
	assert.NoError(err)
	assert.Len(allCpus, 1)
	assert.Len(metadataMap, 1)
	assert.Contains(metadataMap, P4)
	assert.EqualValues(metadata2.Revision+1, metadataMap[P4].Revision, "Revision did not increment by 1 after an update")
	revision, err = p.GetRevision(ctx, P4, &cpuMsg2)
	assert.NoError(err)
	assert.EqualValues(metadata2.Revision+1, revision, "Revision did not increment by 1 after an update")

	// soft delete cpu msg
	rowsAffected, metadata4, err = p.SoftDeleteById(ctx, P4, &pb.CPU{})
	assert.NoError(err, "Failed to delete Protobuf message from ProtoStore")
	assert.EqualValues(1, rowsAffected)
	assert.EqualValues(metadata3.Revision+1, metadata4.Revision, "Revision did not increment by 1 after soft delete")

	cpuMsg6 := pb.CPU{}
	err = p.FindById(ctx, P4, &cpuMsg6, &metadata5)
	assert.ErrorIs(err, ErrRecordNotFound)

	cpuMsg6 = pb.CPU{}
	err = p.FindByIdIncludingSoftDeleted(ctx, P4, &cpuMsg6, &metadata5)
	assert.NoError(err)

	// Query all Protobuf records
	allCpus = make([]pb.CPU, 0)
	metadataMap, err = p.FindAll(ctx, &allCpus, datastore.NoPagination())
	assert.NoError(err)
	assert.Len(allCpus, 0)
	assert.Len(metadataMap, 0)

	allCpus = make([]pb.CPU, 0)
	metadataMap, err = p.FindAllIncludingSoftDeleted(ctx, &allCpus, datastore.NoPagination())
	assert.NoError(err)
	assert.Len(allCpus, 1)
	assert.Len(metadataMap, 1)
	assert.Contains(metadataMap, P4)
	assert.EqualValues(metadata5.Revision, metadataMap[P4].Revision, "Revision did not increment by 1 after an update")

	memMsg1 := pb.Memory{}
	_ = faker.FakeData(&memMsg1)
	if useUpsert {
		// rowsAffected, _, err = p.UpsertWithMetadata(ctx, P4, &memMsg1, Metadata{Revision: 1})
		rowsAffected, _, err = p.Upsert(ctx, P4, &memMsg1)
	} else {
		rowsAffected, _, err = p.Insert(ctx, P4, &memMsg1)
	}
	assert.NoError(err, "Failed to insert a Protobuf message into ProtoStore")
	assert.Equal(int64(1), rowsAffected)

	memMsg2 := pb.Memory{}
	err = p.FindById(ctx, P4, &memMsg2, &metadata1)
	assert.NoError(err, "Failed to find a Protobuf message in ProtoStore")
	assert.Equal(memMsg1.String(), memMsg2.String())
	assert.Equal(P4, metadata1.Id)
	assert.Equal(int64(1), metadata1.Revision)
	revision, err = p.GetRevision(ctx, P4, &pb.Memory{})
	assert.NoError(err)
	assert.Equal(int64(1), revision)

	memMsg3 := pb.Memory{}
	_ = faker.FakeData(&memMsg3)
	if useUpsert {
		rowsAffected, _, err = p.UpsertWithMetadata(ctx, P4, &memMsg3, metadata1)
	} else {
		rowsAffected, _, err = p.UpdateWithMetadata(ctx, P4, &memMsg3, metadata1)
	}
	assert.NoError(err, "Failed to update a Protobuf message in ProtoStore")
	assert.EqualValues(1, rowsAffected)

	memMsg4 := pb.Memory{}
	err = p.FindById(ctx, P4, &memMsg4, nil)
	assert.NoError(err, "Failed to find an updated Protobuf message in ProtoStore")
	assert.Equal(memMsg3.String(), memMsg4.String())

	rowsAffected, err = p.DeleteById(ctx, P4, &pb.CPU{})
	assert.NoError(err, "Failed to delete Protobuf message from ProtoStore")
	assert.EqualValues(1, rowsAffected)

	rowsAffected, err = p.DeleteById(ctx, P4, &pb.CPU{})
	assert.NoError(err, "Deleting a non-existent Protobuf message produced an error. DeleteById() might be not idempotent.")
	assert.EqualValues(0, rowsAffected)

	// Delete CPU message with an ID of P4. Memory message with an ID of P4 must remain intact
	cpuMsg7 := pb.CPU{}
	err = p.FindById(ctx, P4, &cpuMsg7, nil)
	assert.ErrorIs(err, ErrRecordNotFound)
	assert.Equal("", cpuMsg7.String(), "Found a Protobuf message that was supposed to be deleted")

	err = p.FindById(ctx, P4, &memMsg4, nil)
	assert.NoError(err)
	assert.Equal(memMsg3.String(), memMsg4.String(), "Protobuf message that was not supposed to be modified was still modified")

	rowsAffected, _, err = p.SoftDeleteById(ctx, P4, &pb.Memory{})
	assert.NoError(err, "Failed to soft delete Protobuf message from ProtoStore")
	assert.EqualValues(1, rowsAffected)

	rowsAffected, _, err = p.SoftDeleteById(ctx, P4, &pb.Memory{})
	assert.NoError(err, "Soft Deleting a non-existent Protobuf message produced an error. SoftDeleteById() might be not idempotent.")
	assert.EqualValues(0, rowsAffected)

	rowsAffected, err = p.DeleteById(ctx, P4, &pb.Memory{})
	assert.NoError(err, "Failed to delete Protobuf message after soft delete from ProtoStore")
	assert.EqualValues(1, rowsAffected)

	rowsAffected, err = p.DeleteById(ctx, P4, &pb.Memory{})
	assert.NoError(err, "Deleting a non-existent Protobuf message produced an error. DeleteById() might be not idempotent.")
	assert.EqualValues(0, rowsAffected)

	memMsg5 := pb.Memory{}
	err = p.FindById(ctx, P4, &memMsg5, nil)
	assert.ErrorIs(err, ErrRecordNotFound)
	assert.Equal("", memMsg5.String(), "Found a Protobuf message that was supposed to be deleted")
}

/*
Checks if a Protobuf message inserted by a user from Pepsi is visible to the user from Coke.
*/
func TestProtoStoreInDbMultitenancy(t *testing.T) {
	assert := assert.New(t)
	p := setupDbContext(t, "TestProtoStoreInDbMultitenancy")

	cpuMsg1 := pb.CPU{}
	_ = faker.FakeData(&cpuMsg1)
	rowsAffected, md, err := p.Insert(AmericasPepsiAdminCtx, cpuMsg1.Name, &cpuMsg1) // Pepsi inserts a record into Protostore
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(cpuMsg1.Name, md.Id)
	assert.Equal(int64(1), md.Revision)

	queryResult := pb.CPU{}
	err = p.FindById(AmericasCokeAdminCtx, cpuMsg1.Name, &queryResult, nil) // Coke tries to read Pepsi's Protobuf message
	assert.ErrorIs(err, ErrRecordNotFound)                                  // Coke won't be able to see that record due to RLS
	assert.Empty(queryResult.GetName(), "Coke user found a record belonging to Pepsi tenant")
}

func BenchmarkCrudProtoStoreInDb(b *testing.B) {
	log.SetLevel(log.FatalLevel)
	log.SetOutput(io.Discard)
	var t testing.T
	p := setupDbContext(&t, "BenchmarkCrudProtoStoreInDb")
	for n := 0; n < b.N; n++ {
		testProtoStoreCrud(&t, p, AmericasCokeAdminCtx, false)
		testProtoStoreCrud(&t, p, AmericasCokeAdminCtx, true)
	}
}

func TestProtoStoreMsg_String(t *testing.T) {
	assert := assert.New(t)
	p := setupDbContext(t, "TestProtoStoreMsg_String")
	const id = "some-key"
	cpu := &pb.CPU{Brand: "Intel"}
	md := protostore.Metadata{Id: id, Revision: 10}
	orgId, _ := p.GetAuthorizer().GetOrgFromContext(AmericasPepsiAdminCtx)
	pMsg, err := p.MsgToPersist(AmericasPepsiAdminCtx, id, cpu, md)
	if err != nil {
		assert.FailNow("Failed to generate ProtoStoreMsg", err)
	}
	isJson := func(str string) bool { return len(str) >= 2 && str[0] == '{' && str[len(str)-1] == '}' }
	for _, pMsgStr := range []string{pMsg.String(), fmt.Sprintf("%+v", pMsg)} {
		assert.True(isJson(pMsgStr), "Expected string version of ProtoStoreMsg to be a JSON string")
		assert.Contains(pMsgStr, id, "Expected unique ID of ProtoStoreMsg to be printed")
		assert.Contains(pMsgStr, orgId, "Expected org. ID to be printed")
		assert.NotContains(pMsgStr, cpu.Brand, "Expected Protobuf message contents not to be printed")
		assert.NotContains(pMsgStr, "CreatedAt", "Expected Protobuf message contents not to be printed")
		assert.NotContains(pMsgStr, "UpdatedAt", "Expected Protobuf message contents not to be printed")
		assert.NotContains(pMsgStr, "DeletedAt", "Expected Protobuf message contents not to be printed")
	}
	assert.Equal(pMsg.String(), fmt.Sprintf("%+v", pMsg),
		"Expected fmt.Printf(\"%+v\", pMsg) to print the same JSON as pMsg.String()")
}
