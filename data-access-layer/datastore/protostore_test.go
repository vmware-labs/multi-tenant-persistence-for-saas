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
	"io"
	"sort"
	"testing"

	"google.golang.org/protobuf/proto"

	faker "github.com/bxcodec/faker/v3"
	log "github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/test/pb"
)

var protoMsgs = []proto.Message{
	&pb.CPU{},
	&pb.Memory{},
}

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

	copy := s[i]
	s[i] = s[j]
	s[j] = copy
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
	copy := s[i]
	s[i] = s[j]
	s[j] = copy
}

const (
	P1 = "P1"
	P2 = "P2"
	P4 = "P4"
)

func TestProtoConversionWithFaker(t *testing.T) {
	assert := assert.New(t)
	var msg1 = pb.CPU{}
	_ = faker.FakeData(&msg1)
	data, _ := ToBytes(&msg1)
	var msg2 = pb.CPU{}
	_ = FromBytes(data, &msg2)

	assert.Equal(msg1.String(), msg2.String())
}

func TestProtoConversion(t *testing.T) {
	assert := assert.New(t)
	var msg1 = pb.CPU{
		Brand:         "Intel",
		Name:          "Pentium",
		NumberCores:   4,
		NumberThreads: 8,
		MinGhz:        1.6,
	}
	data, _ := ToBytes(&msg1)
	var msg2 = pb.CPU{}
	_ = FromBytes(data, &msg2)

	assert.Equal(msg1.String(), msg2.String())

	var msg3 = pb.Memory{
		Brand: "Samsung",
		Size:  32,
		Speed: 2933,
		Type:  "DDR4",
	}
	data, _ = ToBytes(&msg3)
	var msg4 = pb.Memory{}
	_ = FromBytes(data, &msg4)

	assert.Equal(msg3.String(), msg4.String())
}

func setupDbContext(t *testing.T) {
	t.Helper()
	assert := assert.New(t)
	p := GetProtoStore()

	p.Configure(serviceAdminCtx, false, DataStore.GetAuthorizer())
	if err := p.DropTables(protoMsgs...); err != nil {
		assert.FailNow("Dropping tables failed with error: " + err.Error())
	}

	roleMapping := map[string]DbRole{
		TENANT_AUDITOR:  TENANT_READER,
		TENANT_ADMIN:    TENANT_WRITER,
		SERVICE_AUDITOR: READER,
		SERVICE_ADMIN:   WRITER,
	}
	err := p.Register(serviceAdminCtx, roleMapping, protoMsgs...)
	assert.NoError(err)
}

func setupInMemoryContext(t *testing.T) {
	t.Helper()
	assert := assert.New(t)
	p := GetProtoStore()

	p.Configure(serviceAdminCtx, false, DataStore.GetAuthorizer())
	if err := p.DropTables(protoMsgs...); err != nil {
		assert.FailNow("Dropping tables failed with error: " + err.Error())
	}

	roleMapping := map[string]DbRole{
		TENANT_AUDITOR:  TENANT_READER,
		TENANT_ADMIN:    TENANT_WRITER,
		SERVICE_AUDITOR: READER,
		SERVICE_ADMIN:   WRITER,
	}

	for _, msg := range protoMsgs {
		err := p.Register(serviceAdminCtx, roleMapping, msg)
		assert.NoError(err)
	}
}

func TestProtoStoreInDbFindAll(t *testing.T) {
	setupDbContext(t)
	testProtoStoreFindAll(t, pepsiAdminCtx)
}

func TestProtoStoreInMemoryFindAll(t *testing.T) {
	setupInMemoryContext(t)
	testProtoStoreFindAll(t, cokeAdminCtx)
}

/*
Checks if DIFFERENT types of Protobuf messages can be persisted with the same IDs.
Checks if FindAll() works (both when query results are stored in a ptr to a slice of structs and
when query results are stored in a ptr to a slice of ptrs to structs )
*/
func testProtoStoreFindAll(t *testing.T, ctx context.Context) {
	assert := assert.New(t)
	var p ProtoStore = GetProtoStore()

	//Prepare data for test cases
	var memmsg1, memmsg2, cpumsg1, cpumsg2 = pb.Memory{}, pb.Memory{}, pb.CPU{}, pb.CPU{}
	for _, protoMsg := range []proto.Message{&memmsg1, &memmsg2, &cpumsg1, &cpumsg2} {
		_ = faker.FakeData(protoMsg)
	}
	rowsAffected, md, err := p.Insert(ctx, P1, &memmsg1)
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(P1, md.Id)
	assert.Equal(int64(1), md.Revision)

	rowsAffected, md, err = p.Insert(ctx, P2, &memmsg2)
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(P2, md.Id)
	assert.Equal(int64(1), md.Revision)

	rowsAffected, md, err = p.Insert(ctx, P1, &cpumsg1)
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(P1, md.Id)
	assert.Equal(int64(1), md.Revision)

	rowsAffected, md, err = p.Insert(ctx, P2, &cpumsg2)
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(P2, md.Id)
	assert.Equal(int64(1), md.Revision)

	var expectedMemoryQueryResults MemoryPtrSlice = []*pb.Memory{&memmsg1, &memmsg2}
	var expectedCPUQueryResults CPUPtrSlice = []*pb.CPU{&cpumsg1, &cpumsg2}
	sort.Sort(expectedMemoryQueryResults)
	sort.Sort(expectedCPUQueryResults)

	{
		//Check if you can find all memory records when passing a pointer to slice of structs
		var actualQueryResults MemorySlice = make([]pb.Memory, 0)
		metadataMap, err := p.FindAll(ctx, &actualQueryResults)
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		sort.Sort(actualQueryResults)
		assert.Equal(expectedMemoryQueryResults[0].String(), actualQueryResults[0].String())
		assert.Equal(expectedMemoryQueryResults[1].String(), actualQueryResults[1].String())
	}

	{
		//Check if you can find all memory records when passing a pointer to slice of pointers to structs
		var actualQueryResults MemoryPtrSlice = make([]*pb.Memory, 0)
		metadataMap, err := p.FindAll(ctx, &actualQueryResults)
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		sort.Sort(actualQueryResults)
		assert.Equal(expectedMemoryQueryResults[0].String(), actualQueryResults[0].String())
		assert.Equal(expectedMemoryQueryResults[1].String(), actualQueryResults[1].String())
	}

	{
		//Check if you can find all memory records when passing a pointer to map of strings to structs
		var actualQueryResults = make(map[string]pb.Memory)
		metadataMap, err := p.FindAllAsMap(ctx, actualQueryResults)
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		var expectedQueryResults = make(map[string]pb.Memory)
		expectedQueryResults[P1] = pb.Memory{
			Brand: memmsg1.Brand,
			Size:  memmsg1.Size,
			Speed: memmsg1.Speed,
			Type:  memmsg1.Type,
		}
		expectedQueryResults[P2] = pb.Memory{
			Brand: memmsg2.Brand,
			Size:  memmsg2.Size,
			Speed: memmsg2.Speed,
			Type:  memmsg2.Type,
		}

		for _, id := range []string{P1, P2} {
			var expectedQueryResult = pb.Memory{
				Brand: expectedQueryResults[id].Brand,
				Size:  expectedQueryResults[id].Size,
				Speed: expectedQueryResults[id].Speed,
				Type:  expectedQueryResults[id].Type,
			}
			var actualQueryResult = pb.Memory{
				Brand: actualQueryResults[id].Brand,
				Size:  actualQueryResults[id].Size,
				Speed: actualQueryResults[id].Speed,
				Type:  actualQueryResults[id].Type,
			}
			assert.Equal(expectedQueryResult.String(), actualQueryResult.String())
		}
	}

	{
		//Check if you can find all memory records when passing a pointer to map of strings to pointers to structs
		var actualQueryResults = make(map[string]*pb.Memory)
		metadataMap, err := p.FindAllAsMap(ctx, actualQueryResults)
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		var expectedQueryResults = make(map[string]*pb.Memory)
		expectedQueryResults[P1] = &memmsg1
		expectedQueryResults[P2] = &memmsg2

		for _, id := range []string{P1, P2} {
			var expectedQueryResult, actualQueryResult = expectedQueryResults[id], actualQueryResults[id]
			assert.Equal(expectedQueryResult.String(), actualQueryResult.String())
		}
	}

	{
		//Check if you can find all CPU records when passing a pointer to slice of structs
		var actualQueryResults CPUSlice = make([]pb.CPU, 0)
		metadataMap, err := p.FindAll(ctx, &actualQueryResults)
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		sort.Sort(actualQueryResults)
		assert.Equal(expectedCPUQueryResults[0].String(), actualQueryResults[0].String())
		assert.Equal(expectedCPUQueryResults[1].String(), actualQueryResults[1].String())
	}

	{
		//Check if you can find all CPU records when passing a pointer to slice of pointers to structs
		var actualQueryResults CPUPtrSlice = make([]*pb.CPU, 0)
		metadataMap, err := p.FindAll(ctx, &actualQueryResults)
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		sort.Sort(actualQueryResults)
		assert.Equal(expectedCPUQueryResults[0].String(), actualQueryResults[0].String())
		assert.Equal(expectedCPUQueryResults[1].String(), actualQueryResults[1].String())
	}

	{
		//Check if you can find all CPU records when passing a pointer to map of strings to structs
		var actualQueryResults = make(map[string]pb.CPU)
		metadataMap, err := p.FindAllAsMap(ctx, actualQueryResults)
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		var expectedQueryResults = make(map[string]pb.CPU)
		expectedQueryResults[P1] = pb.CPU{
			Brand:         cpumsg1.Brand,
			Name:          cpumsg1.Name,
			NumberCores:   cpumsg1.NumberCores,
			NumberThreads: cpumsg1.NumberThreads,
			MinGhz:        cpumsg1.MinGhz,
			MaxGhz:        cpumsg1.MaxGhz,
		}
		expectedQueryResults[P2] = pb.CPU{
			Brand:         cpumsg2.Brand,
			Name:          cpumsg2.Name,
			NumberCores:   cpumsg2.NumberCores,
			NumberThreads: cpumsg2.NumberThreads,
			MinGhz:        cpumsg2.MinGhz,
			MaxGhz:        cpumsg2.MaxGhz,
		}

		for _, id := range []string{P1, P2} {
			var expectedQueryResult = pb.CPU{
				Brand:         expectedQueryResults[id].Brand,
				Name:          expectedQueryResults[id].Name,
				NumberCores:   expectedQueryResults[id].NumberCores,
				NumberThreads: expectedQueryResults[id].NumberThreads,
				MinGhz:        expectedQueryResults[id].MinGhz,
				MaxGhz:        expectedQueryResults[id].MaxGhz,
			}
			var actualQueryResult = pb.CPU{
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
		//Check if you can find all CPU records when passing a pointer to map of strings to pointers to structs
		var actualQueryResults = make(map[string]*pb.CPU)
		metadataMap, err := p.FindAllAsMap(ctx, actualQueryResults)
		assert.NoError(err)
		assert.Len(actualQueryResults, 2)
		assert.Len(metadataMap, 2)

		var expectedQueryResults = make(map[string]*pb.CPU)
		expectedQueryResults[P1] = &cpumsg1
		expectedQueryResults[P2] = &cpumsg2

		for _, id := range []string{P1, P2} {
			var expectedQueryResult, actualQueryResult = expectedQueryResults[id], actualQueryResults[id]
			assert.Equal(expectedQueryResult.String(), actualQueryResult.String())
		}
	}
	{
		assert.NotNil(p.GetAuthorizer())
		_, err := p.FindAll(ctx, pb.CPU{})
		assert.ErrorIs(err, IllegalArgumentError)
		_, err = p.FindAll(ctx, &pb.CPU{})
		assert.ErrorIs(err, IllegalArgumentError)
		_, err = p.FindAllAsMap(ctx, pb.CPU{})
		assert.ErrorIs(err, IllegalArgumentError)
		_, err = p.FindAllAsMap(ctx, &pb.CPU{})
		assert.ErrorIs(err, IllegalArgumentError)
	}
	{
		err := p.DropTables(protoMsgs...)
		assert.NoError(err)

		var cpuQueryResults = make(map[string]*pb.CPU)
		_, err = p.FindAllAsMap(ctx, cpuQueryResults)
		assert.ErrorIs(err, ErrorExecutingSqlStmt)

		var memoryQueryResults = make([]*pb.Memory, 0)
		_, err = p.FindAll(ctx, &memoryQueryResults)
		assert.ErrorIs(err, ErrorExecutingSqlStmt)

	}
}

func TestProtoStoreInDbCrud(t *testing.T) {
	setupDbContext(t)
	testProtoStoreCrud(t, pepsiAdminCtx, false)
}

// Same as TestProtoStoreInDbCrud, but uses Upsert instead of Insert or Update
func TestProtoStoreInDbCrudUpsert(t *testing.T) {
	setupDbContext(t)
	testProtoStoreCrud(t, pepsiAdminCtx, true)
}

func TestProtoStoreInMemoryCrud(t *testing.T) {
	setupInMemoryContext(t)
	testProtoStoreCrud(t, cokeAdminCtx, false)
}

// Same as TestProtoStoreInMemoryCrudm but uses Upsert instead of Insert or Update
func TestProtoStoreInMemoryCrudUpsert(t *testing.T) {
	setupInMemoryContext(t)
	testProtoStoreCrud(t, cokeAdminCtx, true)
}

/*
Performs CRUD-operations with ProtoStore.
If useUpsert is true, will use Upsert instead of Insert and Update.
Otherwise, Insert and Update will be used.
*/
func testProtoStoreCrud(t *testing.T, ctx context.Context, useUpsert bool) {
	assert := assert.New(t)
	var p ProtoStore = GetProtoStore()
	var err error
	var rowsAffected int64
	var metadata1, metadata2, metadata3 Metadata

	//Insert Protobuf record
	var cpumsg1 = pb.CPU{}
	_ = faker.FakeData(&cpumsg1)
	if useUpsert {
		rowsAffected, _, err = p.UpsertWithMetadata(ctx, P4, &cpumsg1, Metadata{Revision: 1})
	} else {
		rowsAffected, _, err = p.Insert(ctx, P4, &cpumsg1)
	}
	assert.NoError(err, "Failed to insert a Protobuf message into ProtoStore")
	assert.Equal(int64(1), rowsAffected)

	//Query Protobuf record
	var cpumsg2 = pb.CPU{}
	err = p.FindById(ctx, P4, &cpumsg2, &metadata1)
	assert.NoError(err, "Failed to find a Protobuf message in ProtoStore")
	assert.Equal(cpumsg1.String(), cpumsg2.String())
	if DataStore == &relationalDb { //Only check this for Postgres-backed ProtoStore
		revision, err := p.GetRevision(ctx, P4, &pb.CPU{})
		assert.NoError(err)
		assert.Equal(int64(1), revision)
	}

	//Update Protobuf record (with metadata provided explicitly)
	var cpumsg3 = pb.CPU{}
	_ = faker.FakeData(&cpumsg3)
	if useUpsert {
		rowsAffected, metadata2, err = p.UpsertWithMetadata(ctx, P4, &cpumsg3, metadata1)
	} else {
		rowsAffected, metadata2, err = p.UpdateWithMetadata(ctx, P4, &cpumsg3, metadata1)
	}
	assert.NoError(err, "Failed to update a Protobuf message in ProtoStore")
	assert.EqualValues(1, rowsAffected)
	if DataStore == &relationalDb { //Only check this for Postgres-backed ProtoStore
		assert.EqualValues(metadata1.Revision+1, metadata2.Revision, "Revision did not increment by 1 after an update")
		revision, err := p.GetRevision(ctx, P4, &pb.CPU{})
		assert.NoError(err)
		assert.Equal(metadata1.Revision+1, revision)
	}

	//Query updated Protobuf record
	var cpumsg4 = pb.CPU{}
	err = p.FindById(ctx, P4, &cpumsg4, &metadata2)
	assert.NoError(err, "Failed to find the updated Protobuf message in ProtoStore")
	assert.Equal(cpumsg3.String(), cpumsg4.String())
	if DataStore == &relationalDb { //Only check this for Postgres-backed ProtoStore
		assert.EqualValues(metadata1.Revision+1, metadata2.Revision, "Revision did not increment by 1 after an update")
		revision, err := p.GetRevision(ctx, P4, &pb.CPU{})
		assert.NoError(err)
		assert.Equal(metadata1.Revision+1, revision)
	}

	//Update Protobuf record (without metadata)
	var cpumsg5 = pb.CPU{}
	_ = faker.FakeData(&cpumsg5)
	cpumsg5String := cpumsg5.String()
	if useUpsert {
		rowsAffected, metadata3, err = p.Upsert(ctx, P4, &cpumsg5)
	} else {
		rowsAffected, metadata3, err = p.Update(ctx, P4, &cpumsg5)
	}
	assert.NoError(err, "Failed to update a Protobuf message in ProtoStore")
	assert.EqualValues(1, rowsAffected)
	assert.NotEqual(cpumsg4.String(), cpumsg5.String())
	assert.Equal(cpumsg5String, cpumsg5.String())
	if DataStore == &relationalDb { //Only check this for Postgres-backed ProtoStore
		assert.EqualValues(metadata2.Revision+1, metadata3.Revision, "Revision did not increment by 1 after an update")
		revision, err := p.GetRevision(ctx, P4, &pb.CPU{})
		assert.NoError(err)
		assert.Equal(metadata2.Revision+1, revision)
	}

	//Query all Protobuf records
	var allCpus = make([]pb.CPU, 0)
	metadataMap, err := p.FindAll(ctx, &allCpus)
	assert.NoError(err)
	assert.Len(allCpus, 1)
	if DataStore == &relationalDb { //Only check this for Postgres-backed ProtoStore
		assert.Len(metadataMap, 1)
		assert.Contains(metadataMap, P4)
		assert.EqualValues(metadata2.Revision+1, metadataMap[P4].Revision, "Revision did not increment by 1 after an update")
		revision, err := p.GetRevision(ctx, P4, &cpumsg2)
		assert.NoError(err)
		assert.EqualValues(metadata2.Revision+1, revision, "Revision did not increment by 1 after an update")
	}

	var memmsg1 = pb.Memory{}
	_ = faker.FakeData(&memmsg1)
	if useUpsert {
		//rowsAffected, _, err = p.UpsertWithMetadata(ctx, P4, &memmsg1, Metadata{Revision: 1})
		rowsAffected, _, err = p.Upsert(ctx, P4, &memmsg1)
	} else {
		rowsAffected, _, err = p.Insert(ctx, P4, &memmsg1)
	}
	assert.NoError(err, "Failed to insert a Protobuf message into ProtoStore")
	assert.Equal(int64(1), rowsAffected)

	var memmsg2 = pb.Memory{}
	err = p.FindById(ctx, P4, &memmsg2, &metadata1)
	assert.NoError(err, "Failed to find a Protobuf message in ProtoStore")
	assert.Equal(memmsg1.String(), memmsg2.String())
	if DataStore == &relationalDb { //Only check this for Postgres-backed ProtoStore
		assert.Equal(P4, metadata1.Id)
		assert.Equal(int64(1), metadata1.Revision)
		revision, err := p.GetRevision(ctx, P4, &pb.Memory{})
		assert.NoError(err)
		assert.Equal(int64(1), revision)
	}

	var memmsg3 = pb.Memory{}
	_ = faker.FakeData(&memmsg3)
	if useUpsert {
		rowsAffected, _, err = p.UpsertWithMetadata(ctx, P4, &memmsg3, metadata1)
	} else {
		rowsAffected, _, err = p.UpdateWithMetadata(ctx, P4, &memmsg3, metadata1)
	}
	assert.NoError(err, "Failed to update a Protobuf message in ProtoStore")
	assert.EqualValues(1, rowsAffected)

	var memmsg4 = pb.Memory{}
	err = p.FindById(ctx, P4, &memmsg4, nil)
	assert.NoError(err, "Failed to find an updated Protobuf message in ProtoStore")
	assert.Equal(memmsg3.String(), memmsg4.String())

	rowsAffected, err = p.DeleteById(ctx, P4, &pb.CPU{})
	assert.NoError(err, "Failed to delete Protobuf message from ProtoStore")
	assert.EqualValues(1, rowsAffected)

	rowsAffected, err = p.DeleteById(ctx, P4, &pb.CPU{})
	assert.NoError(err, "Deleting a non-existent Protobuf message produced an error. DeleteById() might be not idempotent.")
	assert.EqualValues(0, rowsAffected)

	//Delete CPU message with an ID of P4. Memory message with an ID of P4 must remain intact
	var cpumsg6 = pb.CPU{}
	err = p.FindById(ctx, P4, &cpumsg6, nil)
	assert.NoError(err)
	assert.Equal("", cpumsg6.String(), "Found a Protobuf message that was supposed to be deleted")

	err = p.FindById(ctx, P4, &memmsg4, nil)
	assert.NoError(err)
	assert.Equal(memmsg3.String(), memmsg4.String(), "Protobuf message that was not supposed to be modified was still modified")

	rowsAffected, err = p.DeleteById(ctx, P4, &pb.Memory{})
	assert.NoError(err, "Failed to delete Protobuf message from ProtoStore")
	assert.EqualValues(1, rowsAffected)

	rowsAffected, err = p.DeleteById(ctx, P4, &pb.Memory{})
	assert.NoError(err, "Deleting a non-existent Protobuf message produced an error. DeleteById() might be not idempotent.")
	assert.EqualValues(0, rowsAffected)

	var memmsg5 = pb.Memory{}
	err = p.FindById(ctx, P4, &memmsg5, nil)
	assert.NoError(err)
	assert.Equal("", memmsg5.String(), "Found a Protobuf message that was supposed to be deleted")
}

/*
Checks if a Protobuf message inserted by a user from Pepsi is visible to the user from Coke.
*/
func TestProtoStoreInDbMultitenancy(t *testing.T) {
	assert := assert.New(t)

	var p ProtoStore = GetProtoStore()
	var cpumsg1 = pb.CPU{}
	_ = faker.FakeData(&cpumsg1)
	rowsAffected, md, err := p.Insert(pepsiAdminCtx, cpumsg1.Name, &cpumsg1) //Pepsi inserts a record into Protostore
	assert.NoError(err)
	assert.Equal(int64(1), rowsAffected)
	assert.Equal(cpumsg1.Name, md.Id)
	assert.Equal(int64(1), md.Revision)

	var queryResult = pb.CPU{}
	err = p.FindById(cokeAdminCtx, cpumsg1.Name, &queryResult, nil)
	assert.NoError(err)
	assert.Empty(queryResult.GetName(), "Coke user found a record belonging to Pepsi tenant")
}

func BenchmarkCrudProtoStoreInDb(b *testing.B) {
	log.SetLevel(log.FatalLevel)
	log.SetOutput(io.Discard)
	var t testing.T
	setupDbContext(&t)
	for n := 0; n < b.N; n++ {
		testCrud(&t, cokeAdminCtx)
	}
}

func BenchmarkCrudProtoStoreInMemory(b *testing.B) {
	log.SetLevel(log.FatalLevel)
	log.SetOutput(io.Discard)
	var t testing.T
	setupInMemoryContext(&t)
	for n := 0; n < b.N; n++ {
		testCrud(&t, pepsiAdminCtx)
	}
}
