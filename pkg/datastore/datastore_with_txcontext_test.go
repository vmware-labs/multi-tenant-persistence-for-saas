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
	"database/sql"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/test"
)

func rollbackTx(t *testing.T, tx *gorm.DB) {
	t.Helper()
	if err := tx.Rollback().Error; err != nil && err != sql.ErrTxDone {
		t.Log("Rollback of tx errored", err)
	}
}

func testTxCrud(t *testing.T, ds datastore.DataStore, ctx context.Context, myCokeApp *App, user1, user2 *AppUser) {
	t.Helper()
	assert := assert.New(t)
	var err error

	txFetcher := authorizer.SimpleTransactionFetcher{}

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

	// TODO: Find, Update and Delete are still not supported due to appending where clauses,
	// after each call to Find/Update/Delete methods

	tx, err := ds.Helper().GetDBTransaction(ctx, datastore.GetTableName(user1), user1)
	assert.NoError(err)
	defer rollbackTx(t, tx)
	txCtx := txFetcher.WithTransactionCtx(ctx, tx)
	for _, record := range []*AppUser{user1, user2} {
		rowsAffected, err := ds.Upsert(txCtx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)
	}
	assert.NoError(tx.Commit().Error)

	for _, record := range []*AppUser{user1, user2} {
		queryResult := &AppUser{Id: record.Id}
		err = ds.Find(ctx, queryResult)
		assert.NoError(err)
		assert.Equal(record, queryResult)
	}

	tx, err = ds.Helper().GetDBTransaction(ctx, datastore.GetTableName(user1), user1)
	assert.NoError(err)
	defer rollbackTx(t, tx)
	txCtx = txFetcher.WithTransactionCtx(ctx, tx)
	// Upsert operation should be an update for already existing records
	user1.NumFollowers++
	user2.NumFollowers--
	for _, record := range []*AppUser{user1, user2} {
		rowsAffected, err := ds.Upsert(txCtx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)
	}
	assert.NoError(tx.Commit().Error)
	for _, record := range []*AppUser{user1, user2} {
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

func BenchmarkTxCrud(b *testing.B) {
	logger := datastore.GetLogger()
	logger.SetLevel(logrus.FatalLevel)
	logger.SetOutput(io.Discard)
	LOG = logger.WithField(datastore.COMP, datastore.SAAS_PERSISTENCE)

	var t testing.T
	ds, _ := SetupDataStore("BenchmarkCrud")
	myCokeApp, user1, user2 := SetupDbTables(ds)
	for n := 0; n < b.N; n++ {
		testCrud(&t, ds, CokeAdminCtx, myCokeApp, user1, user2)
	}
}

func TestTxCrud(t *testing.T) {
	ds, _ := SetupDataStore("TestTxCrud")
	myCokeApp, user1, user2 := SetupDbTables(ds)
	testTxCrud(t, ds, CokeAdminCtx, myCokeApp, user1, user2)
}
