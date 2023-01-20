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
	"os"
)

/*
Datastore: Interface to be implemented by the persistence library.
*/
type dataStore interface {
	GetAuthorizer() Authorizer
	Configure(ctx context.Context, isDataStoreInMemory bool, authorizer Authorizer)
	PerformJoinOneToMany(ctx context.Context, record1 Record, record1Id string, record2JoinOnColumn string, query2Output interface{}) error
	PerformJoinOneToOne(ctx context.Context, record1 Record, record1Id string, record2 Record, record2JoinOnColumn string) error
	Find(ctx context.Context, record Record) error
	FindAll(ctx context.Context, records interface{}) error
	FindWithFilter(ctx context.Context, record Record, records interface{}) error
	Insert(ctx context.Context, record Record) (int64, error)
	Delete(ctx context.Context, record Record) (int64, error)
	Update(ctx context.Context, record Record) (int64, error)
	Upsert(ctx context.Context, record Record) (int64, error)
	RegisterWithDAL(ctx context.Context, roleMapping map[string]DbRole, record Record) error
	Reset()
}

type DataStoreHelper interface {
	GetAuthorizer() Authorizer
	Configure(ctx context.Context, isDataStoreInMemory bool, authorizer Authorizer)
	RegisterWithDALHelper(ctx context.Context, roleMapping map[string]DbRole, tableName string, record Record) error
	FindInTable(ctx context.Context, tableName string, record Record) error
	FindAllInTable(ctx context.Context, tableName string, records interface{}) error
	FindWithFilterInTable(ctx context.Context, tableName string, record Record, records interface{}) error
	InsertInTable(ctx context.Context, tableName string, record Record) (int64, error)
	UpdateInTable(ctx context.Context, tableName string, record Record) (int64, error)
	UpsertInTable(ctx context.Context, tableName string, record Record) (int64, error)
	DeleteInTable(ctx context.Context, tableName string, record Record) (int64, error)
}

type DataStoreTestHelper interface {
	DropTables(records ...Record) error                       // Drop DB tables by records
	Drop(tableNames ...string) error                          // Drops DB tables
	DropCascade(cascade bool, tableNames ...string) error     // Drops DB tables, with an option to drop them in a cascading fashion
	Truncate(tableNames ...string) error                      // Truncates DB tables
	TruncateCascade(cascade bool, tableNames ...string) error // Truncates DB tables, with an option to truncate them in a cascading fashion
	DoesTableExist(tableName string) error                    // Checks if table exists
}

var (
	DataStore  dataStore           = &relationalDb
	Helper     DataStoreHelper     = &relationalDb
	TestHelper DataStoreTestHelper = &relationalDb
)

func configureDataStore(isDataStoreInMemory bool, authorizer Authorizer) {
	if authorizer == nil {
		os.Exit(1)
	}
	if isDataStoreInMemory {
		if authorizer != nil {
			inMemoryDataStore.authorizer = authorizer
		}
		DataStore = &inMemoryDataStore
		DatastoreHelperInMemory()
	} else {
		if authorizer != nil {
			relationalDb.authorizer = authorizer
		}
		DataStore = &relationalDb
		DatastoreHelperInDatabase()
	}
}

func DatastoreHelperInMemory() DataStoreHelper {
	Helper = &inMemoryDataStore
	TestHelper = &inMemoryDataStore
	return Helper
}

func DatastoreHelperInDatabase() DataStoreHelper {
	Helper = &relationalDb
	TestHelper = &relationalDb
	return Helper
}
