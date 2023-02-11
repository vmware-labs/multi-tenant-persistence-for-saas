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

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	"gorm.io/gorm"
)

// DataStore /*.
type DataStore interface {
	GetAuthorizer() authorizer.Authorizer
	Find(ctx context.Context, record Record) error
	FindAll(ctx context.Context, records interface{}) error
	FindWithFilter(ctx context.Context, record Record, records interface{}) error
	Insert(ctx context.Context, record Record) (int64, error)
	Delete(ctx context.Context, record Record) (int64, error)
	Update(ctx context.Context, record Record) (int64, error)
	Upsert(ctx context.Context, record Record) (int64, error)
	RegisterWithDAL(ctx context.Context, roleMapping map[string]dbrole.DbRole, record Record) error
	Reset()
	Helper() Helper
	TestHelper() TestHelper
	GetTransaction(ctx context.Context, record ...Record) (tx *gorm.DB, err error)
}

type Helper interface {
	GetAuthorizer() authorizer.Authorizer
	GetDBTransaction(ctx context.Context, tableName string, record Record) (tx *gorm.DB, err error)
	RegisterWithDALHelper(ctx context.Context, roleMapping map[string]dbrole.DbRole, tableName string, record Record) error
	FindInTable(ctx context.Context, tableName string, record Record) error
	FindAllInTable(ctx context.Context, tableName string, records interface{}) error
	FindWithFilterInTable(ctx context.Context, tableName string, record Record, records interface{}) error
	InsertInTable(ctx context.Context, tableName string, record Record) (int64, error)
	UpdateInTable(ctx context.Context, tableName string, record Record) (int64, error)
	UpsertInTable(ctx context.Context, tableName string, record Record) (int64, error)
	DeleteInTable(ctx context.Context, tableName string, record Record) (int64, error)
}

type TestHelper interface {
	DropTables(records ...Record) error                       // Drop DB tables by records
	Truncate(tableNames ...string) error                      // Truncates DB tables
	TruncateCascade(cascade bool, tableNames ...string) error // Truncates DB tables, with an option to truncate them in a cascading fashion
}
