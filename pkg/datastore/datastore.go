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

// Import the package and use `DataStore` interface to interact with the data access
// layer. If you want DAL to use a Postgres database, ensure you have the following
// environment variables set to relevant values: *DB_ADMIN_USERNAME*, *DB_PORT*,
// *DB_NAME*, *DB_ADMIN_PASSWORD*, *DB_HOST*, *SSL_MODE*. You can also set
// *LOG_LEVEL* environment variable to debug/trace, if you want logging at a
// specific level (default is _Info_)
//
// Define structs that will be persisted using datastore similar to any gorm Models,
// for reference [https://gorm.io/docs/models.html]
//
// - At least one field must be a primary key (contain a *primary_key* tag with the value of *true*).
// - For revision support to block concurrent updates, add __revision_ as column tag.
// - For multi-tenancy support, add an _org_id_ tag to a field.
//
//		type App struct {
//			Id       string `gorm:"primaryKey;column:application_id"`
//			Name     string
//			TenantId string `gorm:"primaryKey;column:org_id"`
//			Revision int64  `gorm:"column:revision"`
//		}
//
//	DataStore.Register(context.TODO(), roleMappingForAppUser, appUser{})
//
//	datastore.DataStore.Insert(ctx, user1)
//	var queryResult appUser = appUser{Id: user1.Id}
//	DataStore.Find(ctx, &queryResult)
//
//	queryResults := make([]appUser, 0)
//	DataStore.FindAll(ctx, &queryResults)
//
//	DataStore.Delete(ctx, user1)
//
// DataStore interface exposes basic methods like Find/FindAll/Upsert/Delete, for richer queries
// and transaction based filtering and pagination please use GetTransaction() method.
// Sample usage using db transactions,
//
//	t.Log("Getting DBTransaction for creating App and AppUser")
//	tx, err := TestDataStore.GetTransaction(CokeAdminCtx, app, appUser)
//	assert.NoError(err)
//	t.Log("Creating app and appUser in single transaction")
//	err = tx.Transaction(func(tx *gorm.DB) error {
//		if err := tx.Create(app).Error; err != nil {
//			return err
//		}
//		if err := tx.Create(appUser).Error; err != nil {
//			return err
//		}
//
//		return nil
//	})
//	tx.Commit()
package datastore

import (
	"context"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	"gorm.io/gorm"
)

// DataStore /*.
type DataStore interface {
	Find(ctx context.Context, record Record) error
	FindAll(ctx context.Context, records interface{}, pagination *Pagination) error
	FindWithFilter(ctx context.Context, filter Record, records interface{}, pagination *Pagination) error
	Insert(ctx context.Context, record Record) (int64, error)
	Delete(ctx context.Context, record Record) (int64, error)
	Update(ctx context.Context, record Record) (int64, error)
	Upsert(ctx context.Context, record Record) (int64, error)
	GetTransaction(ctx context.Context, record ...Record) (tx *gorm.DB, err error)

	Register(ctx context.Context, roleMapping map[string]dbrole.DbRole, records ...Record) error
	Reset()

	GetAuthorizer() authorizer.Authorizer
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
