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
	"fmt"
	"reflect"
	"sync"
)

/*
Implementation of DataStore backed by an in-memory cache. By default, uses MetadataBasedAuthorizer, which permits all operations on any tenant's data.
This implementation does not have a notion of multi-tenancy - all the methods can access/modify all tenant's data.
*/
type InMemory struct {
	caches        map[string](map[string]Record) // Map of maps, where key is a name of the persisted objects and value is
	cacheMutexes  map[string]*sync.RWMutex       // Protects a cache from concurrent modifications
	cacheMapMutex sync.RWMutex                   // Protects caches map from concurrent modifications
	authorizer    Authorizer                     // MetadataBasedAuthorizer by default
}

var inMemoryDataStore InMemory = InMemory{
	caches:        make(map[string](map[string]Record)),
	cacheMutexes:  make(map[string]*sync.RWMutex),
	cacheMapMutex: sync.RWMutex{},
	authorizer:    MetadataBasedAuthorizer{}, // dummy authorizer that allows all operations and just used to extract org. ID from context
}

func (inMemory *InMemory) SetAuthorizer(authorizer Authorizer) {
	if authorizer != nil {
		inMemory.authorizer = authorizer
	}
}

func (inMemory *InMemory) GetAuthorizer() Authorizer {
	return inMemory.authorizer
}

/*
Reset all the caches in memory and mutexes
*/
func (inMemory *InMemory) Reset() {
	inMemory.caches = make(map[string](map[string]Record))
	inMemory.cacheMutexes = make(map[string]*sync.RWMutex)
	inMemory.cacheMapMutex = sync.RWMutex{}
}

/*
Initializes the in-memory caches and their mutexes
*/
func (inMemory *InMemory) initialize(cacheName string) {
	// If registered caches map hasn't been initialized, do so
	if inMemory.caches == nil {
		inMemory.cacheMapMutex.Lock()
		defer inMemory.cacheMapMutex.Unlock()
		inMemory.caches = make(map[string](map[string]Record))
	}

	// Register the requested cache if necessary
	if _, isCacheRegistered := inMemory.caches[cacheName]; !isCacheRegistered {
		inMemory.cacheMutexes[cacheName] = new(sync.RWMutex)
		inMemory.cacheMutexes[cacheName].Lock()
		defer inMemory.cacheMutexes[cacheName].Unlock()
		inMemory.caches[cacheName] = make(map[string]Record)
	}
}

/*
Performs an inner join of in-memory caches for records record1 and record2.
Assumes the relationship between record1 and record2 is one-to-one.
Stores the retrieved record from the first cache in record1 argument, and the matching
records from the second cache in query2Output argument.
*/
func (inMemory *InMemory) PerformJoinOneToOne(ctx context.Context, record1 Record, record1Id string, record2 Record, record2JoinOnColumn string) error {
	if err := validateIdAndRecordPtr(record1Id, record1); err != nil {
		return err
	}

	if err := validateFieldAndRecordPtr(record2JoinOnColumn, "record2JoinOnColumn", record2); err != nil {
		return err
	}

	// Find a record from the first cache by ID
	err := inMemory.Find(ctx, record1)
	if err != nil {
		return err
	}

	// Find the first matching record from the 2nd cache
	cache2Name := reflect.ValueOf(record2).Elem().Type().Name()
	inMemory.initialize(cache2Name)

	inMemory.cacheMutexes[cache2Name].RLock()
	defer inMemory.cacheMutexes[cache2Name].RUnlock()
	cache := inMemory.caches[cache2Name]

	// Perform "join" by finding the record which has record2JoinOnColumn equal to first record's ID
	for _, sourceRecord := range cache {
		if getField(record2JoinOnColumn, sourceRecord) != record1.GetId()[0] {
			continue
		}

		copyRecord(sourceRecord, record2)
		break
	}
	return nil
}

/*
Performs an inner join of in-memory caches for records record1 and record2.
Assumes the relationship between record1 and record2 is one-to-many.
Stores the retrieved record from the first cache in record1 argument, and the matching
records from the second cache in query2Output argument.
*/
// TODO - if the first cache contains the record but the second one doesn't, don't return anything
func (inMemory *InMemory) PerformJoinOneToMany(ctx context.Context, record1 Record, record1Id string, record2JoinOnColumn string, query2Output interface{}) error {
	if err := validateIdAndRecordPtr(record1Id, record1); err != nil {
		return err
	}

	if err := validateFieldAndRecordSlicePtr(record2JoinOnColumn, "record2JoinOnColumn", query2Output, 1); err != nil {
		return err
	}

	record2Type := reflect.ValueOf(query2Output).Elem().Index(0).Type()
	record2 := reflect.New(record2Type).Interface().(Record)

	struct2Type := reflect.ValueOf(record2).Elem().Type()
	matchingRecords2 := reflect.MakeSlice(reflect.SliceOf(struct2Type), 0, 5 /* initial capacity */)

	// Find a record from the first cache by ID
	err := inMemory.Find(ctx, record1)
	if err != nil {
		reflect.ValueOf(query2Output).Elem().Set(matchingRecords2)
		return err
	}

	// Find all records from the second cache
	err = inMemory.FindAll(ctx, query2Output)
	if err != nil {
		reflect.ValueOf(query2Output).Elem().Set(matchingRecords2)
		return err
	}

	query2OutputValue := reflect.ValueOf(query2Output).Elem()
	var size = query2OutputValue.Len()
	for i := 0; i < size; i++ {
		record := query2OutputValue.Index(i)

		// Performing a "join" of record1 and record2
		if record1Id == getField(record2JoinOnColumn, record.Interface().(Record)) {
			matchingRecords2 = reflect.Append(matchingRecords2, record)
		}
	}

	reflect.ValueOf(query2Output).Elem().Set(matchingRecords2)
	return nil
}

/*
Finds a record that has the same primary key as record.
record must be a pointer to a struct.
Returns RecordNotFoundError if a record could not be found.
*/
func (inMemory *InMemory) Find(ctx context.Context, record Record) error {
	return inMemory.FindInTable(ctx, GetTableName(record), record)
}

func (inMemory *InMemory) FindInTable(ctx context.Context, cacheName string, record Record) error {
	var id string = record.GetId()[0].(string)
	if err := validateIdAndRecordPtr(id, record); err != nil {
		return err
	}

	inMemory.initialize(cacheName)

	inMemory.cacheMutexes[cacheName].RLock()
	defer inMemory.cacheMutexes[cacheName].RUnlock()
	cache := inMemory.caches[cacheName]

	queryResult, ok := cache[id]
	if !ok {
		return RecordNotFoundError.WithValue(PRIMARY_KEY, fmt.Sprintf("%v", record.GetId()))
	}

	copyRecord(queryResult, record)
	return nil
}

/*
Finds all records of the requested type.
record must be a pointer to a struct.
records must be a pointer to a slice of structs.
*/
func (inMemory *InMemory) FindAll(ctx context.Context, records interface{}) error {
	if reflect.TypeOf(records).Kind() != reflect.Ptr || reflect.TypeOf(records).Elem().Kind() != reflect.Slice {
		errMsg := "\"records\" argument has to be a pointer to a slice of structs implementing \"Record\" interface"
		logger.Error(errMsg)
		err := IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}

	tableName := GetTableNameFromSlice(records)
	return inMemory.FindAllInTable(ctx, tableName, records)
}

func (inMemory *InMemory) FindAllInTable(ctx context.Context, cacheName string, records interface{}) error {
	record := GetRecordInstanceFromSlice(records)
	return inMemory.FindWithFilterInTable(ctx, cacheName, record, records)
}

/*
Finds all records of the requested type. Does not do any filtering.
record must be a pointer to a struct.
records must be a pointer to a slice of structs.
*/
func (inMemory *InMemory) FindWithFilter(ctx context.Context, record Record, records interface{}) error {
	return inMemory.FindWithFilterInTable(ctx, GetTableName(record), record, records)
}
func (inMemory *InMemory) FindWithFilterInTable(ctx context.Context, cacheName string, record Record, records interface{}) error {
	// Validate arguments
	if reflect.TypeOf(records).Kind() != reflect.Ptr || reflect.TypeOf(records).Elem().Kind() != reflect.Slice {
		errMsg := "\"records\" argument has to be a pointer to a slice of structs implementing \"Record\" interface"
		logger.Error(errMsg)
		err := IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return err
	}

	structValue := reflect.ValueOf(record)
	if structValue.Kind() == reflect.Ptr {
		structValue = structValue.Elem()
	}

	structType := structValue.Type()
	inMemory.initialize(cacheName)

	inMemory.cacheMutexes[cacheName].RLock()
	defer inMemory.cacheMutexes[cacheName].RUnlock()
	cache := inMemory.caches[cacheName]

	output := reflect.MakeSlice(reflect.SliceOf(structType), 0, 5 /* initial capacity */)
	for _, sourceRecord := range cache {
		recordCopy := reflect.New(structType).Interface().(Record)
		copyRecord(sourceRecord, recordCopy)
		output = reflect.Append(output, reflect.ValueOf(recordCopy).Elem())
	}

	reflect.ValueOf(records).Elem().Set(output)
	return nil
}

/*
Inserts a record into cache.
record must be a struct.
*/
func (inMemory *InMemory) Insert(ctx context.Context, record Record) (int64, error) {
	return inMemory.InsertInTable(ctx, GetTableName(record), record)
}
func (inMemory *InMemory) InsertInTable(_ context.Context, cacheName string, record Record) (int64, error) {
	var id string = record.GetId()[0].(string)
	if err := validateIdAndRecordStruct(id, record); err != nil {
		return 0, err
	}

	inMemory.initialize(cacheName)

	inMemory.cacheMutexes[cacheName].Lock()
	defer inMemory.cacheMutexes[cacheName].Unlock()
	cache := inMemory.caches[cacheName]

	cache[id] = record
	return 1, nil
}

/*
Deletes a record from cache.
record must be a struct.
*/
func (inMemory *InMemory) Delete(ctx context.Context, record Record) (int64, error) {
	return inMemory.DeleteInTable(ctx, GetTableName(record), record)
}

func (inMemory *InMemory) DeleteInTable(ctx context.Context, cacheName string, record Record) (int64, error) {
	var id string = record.GetId()[0].(string)
	if err := validateIdAndRecordStruct(id, record); err != nil {
		return 0, err
	}

	inMemory.initialize(cacheName)

	inMemory.cacheMutexes[cacheName].Lock()
	defer inMemory.cacheMutexes[cacheName].Unlock()
	cache := inMemory.caches[cacheName]

	var rowsAffected int64 = 0
	if _, ok := cache[id]; ok {
		rowsAffected = 1
	}
	delete(cache, id)
	return rowsAffected, nil
}

func (inMemory *InMemory) clearCache(tableName string) {
	if _, present := inMemory.cacheMutexes[tableName]; !present {
		return
	}

	inMemory.cacheMutexes[tableName].Lock()
	inMemory.caches[tableName] = make(map[string]Record)
	inMemory.cacheMutexes[tableName].Unlock()
}

func (inMemory *InMemory) DropTables(records ...Record) error {
	for _, record := range records {
		inMemory.clearCache(GetTableName(record))
	}

	return nil
}

func (inMemory *InMemory) Drop(tableNames ...string) error {
	inMemory.Reset()
	return nil
}

/*
Deletes all records from cache
*/
func (inMemory *InMemory) Truncate(tableNames ...string) error {
	inMemory.cacheMapMutex.Lock()
	defer inMemory.cacheMapMutex.Unlock()

	for tableName := range inMemory.caches {
		inMemory.clearCache(tableName)
	}
	return nil
}

/*
Updates a record in cache.
record must be a struct.
*/
func (inMemory *InMemory) Update(ctx context.Context, record Record) (int64, error) {
	return inMemory.UpdateInTable(ctx, GetTableName(record), record)
}

func (inMemory *InMemory) UpdateInTable(ctx context.Context, cacheName string, record Record) (int64, error) {
	var id string = record.GetId()[0].(string)
	if err := validateIdAndRecordStruct(id, record); err != nil {
		return 0, err
	}

	inMemory.initialize(cacheName)

	inMemory.cacheMutexes[cacheName].Lock()
	defer inMemory.cacheMutexes[cacheName].Unlock()
	cache := inMemory.caches[cacheName]

	if _, ok := cache[id]; ok {
		// Record is in the cache. Updating it
		cache[id] = record
		return 1, nil
	} else {
		return 0, nil
	}
}

func (inMemory *InMemory) Upsert(ctx context.Context, record Record) (int64, error) {
	return inMemory.UpsertInTable(ctx, GetTableName(record), record)
}

func (inMemory *InMemory) UpsertInTable(ctx context.Context, cacheName string, record Record) (int64, error) {
	var id string = record.GetId()[0].(string)
	if err := validateIdAndRecordStruct(id, record); err != nil {
		return 0, err
	}

	inMemory.initialize(cacheName)
	cache := inMemory.caches[cacheName]

	if _, present := cache[id]; !present {
		// Record doesn't exist in cache - will create it
		return inMemory.InsertInTable(ctx, cacheName, record)
	} else {
		// Record already exists in cache - will update it
		return inMemory.UpdateInTable(ctx, cacheName, record)
	}
}

func (inMemory *InMemory) RegisterWithDAL(ctx context.Context, roleMapping map[string]DbRole, record Record) error {
	return inMemory.RegisterWithDALHelper(ctx, roleMapping, GetTableName(record), record)
}
func (inMemory *InMemory) RegisterWithDALHelper(_ context.Context, roleMapping map[string]DbRole, cacheName string, record Record) error {
	logger.Debugf("Registering the struct %q with DAL (backed by an in-memory cache)... Using authorizer %s...", cacheName, GetTableName(inMemory.authorizer))

	_ = getCreateTableStmt(cacheName, record)
	_ = getColumnNames(cacheName, record) // Fill up caches in sql_struct.go
	return nil
}

/*
Configures data store to use Postgres or an in-memory cache
*/
func (inMemory *InMemory) Configure(_ context.Context, isDataStoreInMemory bool, authorizer Authorizer) {
	configureDataStore(isDataStoreInMemory, authorizer)
}

/*
Copies contents of source, which is a struct, to dest, which is a pointer to a struct
*/
func copyRecord(source, dest Record) {
	destRecordValue := reflect.ValueOf(dest).Elem()
	sourceRecordValue := reflect.ValueOf(source)
	for i := 0; i < destRecordValue.NumField(); i++ {
		switch destRecordValue.Field(i).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			intValue := sourceRecordValue.Field(i).Int()
			destRecordValue.Field(i).SetInt(intValue)
		case reflect.String:
			stringValue := sourceRecordValue.Field(i).String()
			destRecordValue.Field(i).SetString(stringValue)
		case reflect.Bool:
			boolValue := sourceRecordValue.Field(i).Bool()
			destRecordValue.Field(i).SetBool(boolValue)
		case reflect.Slice:
			bytesValue := sourceRecordValue.Field(i).Bytes()
			destRecordValue.Field(i).SetBytes(bytesValue)
		}
	}
}
