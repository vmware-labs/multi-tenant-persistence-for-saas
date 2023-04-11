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

/*
Import the package and use `DataStore` interface to interact with the data access
layer. If you want DAL to use a Postgres database, ensure you have the following
environment variables set to relevant values: `DB_ADMIN_USERNAME`, `DB_PORT`,
`DB_NAME`, `DB_ADMIN_PASSWORD`, `DB_HOST`, `SSL_MODE`. You can also set
`LOG_LEVEL` environment variable to debug/trace, if you want logging at a
specific level (default is `Info`)

Define structs that will be persisted using datastore similar to any gorm Models,
for reference https://gorm.io/docs/models.html

  - At least one field must be a primary key with `gorm:"primaryKey"` tag
  - For multi-tenancy support, add `gorm:"column:org_id"` as tag to a filed
  - For revision support to block concurrent updates, add `gorm:"column:revision"` as tag
  - For multi-instance support, add `gorm:"column:instance_id"` as tag

DataStore interface exposes basic methods like Find/FindAll/Upsert/Delete. For richer queries
and performing a set of operations within a transaction, please, use GetTransaction() method.
For more info, refer to Gorm's transactions page: https://gorm.io/docs/transactions.html
*/
package datastore

import (
	"context"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	"gorm.io/gorm"
)

type DataStore interface {
	Find(ctx context.Context, record Record) error
	FindAll(ctx context.Context, records interface{}, pagination *Pagination) error
	FindWithFilter(ctx context.Context, filter Record, records interface{}, pagination *Pagination) error
	Insert(ctx context.Context, record Record) (int64, error)
	SoftDelete(ctx context.Context, record Record) (int64, error)
	Delete(ctx context.Context, record Record) (int64, error)
	Update(ctx context.Context, record Record) (int64, error)
	Upsert(ctx context.Context, record Record) (int64, error)
	GetTransaction(ctx context.Context, record ...Record) (tx *gorm.DB, err error)

	// Create a DB table for the given struct. Enables RLS in it if it is multi-tenant.
	// Generates Postgres roles and policies based on the provided role mapping and applies them
	// to the created DB table.
	// roleMapping - maps service roles to DB roles to be used for the generated DB table
	// There are 4 possible DB roles to choose from:
	// - READER, which gives read access to all the records in the table
	// - WRITER, which gives read & write access to all the records in the table
	// - TENANT_READER, which gives read access to current tenant's records
	// - TENANT_WRITER, which gives read & write access to current tenant's records.
	Register(ctx context.Context, roleMapping map[string]dbrole.DbRole, records ...Record) error
	Reset()

	GetAuthorizer() authorizer.Authorizer
	GetInstancer() authorizer.Instancer
	Helper() Helper
	TestHelper() TestHelper
}

type Helper interface {
	FindAllInTable(ctx context.Context, tableName string, records interface{}, pagination *Pagination) error
	FindWithFilterInTable(ctx context.Context, tableName string, record Record, records interface{}, pagination *Pagination) error
	GetDBTransaction(ctx context.Context, tableName string, record Record) (tx *gorm.DB, err error)

	RegisterHelper(ctx context.Context, roleMapping map[string]dbrole.DbRole, tableName string, record Record) error
}

type TestHelper interface {
	DropTables(records ...Record) error                       // Drop DB tables by records
	Truncate(tableNames ...string) error                      // Truncates DB tables
	TruncateCascade(cascade bool, tableNames ...string) error // Truncates DB tables, with an option to truncate them in a cascading fashion
}
