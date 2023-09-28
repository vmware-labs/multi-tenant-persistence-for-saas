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
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/logutils"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/test"
)

func rollbackTx(t *testing.T, tx *gorm.DB) {
	t.Helper()
	if err := tx.Rollback().Error; err != nil && err != sql.ErrTxDone {
		t.Log("Rollback of tx errored", err)
	}
}

func testTxCrud(t *testing.T, ds datastore.DataStore, ctx context.Context, user1, user2 *AppUser) {
	t.Helper()
	assert := assert.New(t)
	var err error

	txFetcher := authorizer.SimpleTransactionFetcher{}

	t.Log("Querying created records in single transaction")
	tx, err := ds.GetTransaction(ctx, user1)
	assert.NoError(err)
	defer rollbackTx(t, tx)
	txCtx := txFetcher.WithTransactionCtx(ctx, tx)
	for _, record := range []*AppUser{user1, user2} {
		queryResult := &AppUser{Id: record.Id}
		err = ds.Find(txCtx, queryResult)
		assert.NoError(err)
		assert.True(cmp.Equal(record, queryResult, cmpopts.IgnoreFields(AppUser{}, "CreatedAt", "UpdatedAt", "DeletedAt")))
	}
	assert.NoError(tx.Commit().Error)

	t.Log("Verifying update and find in single transaction")
	user1.Name = "Jeyhun G."
	user1.Email = "jeyhun111@mail.com"
	user1.EmailConfirmed = !user1.EmailConfirmed
	user1.NumFollowers++
	user2.Name = "Jahangir G."
	user2.Email = "jahangir111@mail.com"
	user2.EmailConfirmed = !user2.EmailConfirmed
	user2.NumFollowers--
	tx, err = ds.GetTransaction(ctx, user1)
	assert.NoError(err)
	defer rollbackTx(t, tx)
	txCtx = txFetcher.WithTransactionCtx(ctx, tx)
	for _, record := range []*AppUser{user1, user2} {
		rowsAffected, err := ds.Update(txCtx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)

		queryResult := &AppUser{Id: record.Id}
		err = ds.Find(txCtx, queryResult)
		assert.NoError(err)
		assert.True(cmp.Equal(record, queryResult, cmpopts.IgnoreFields(AppUser{}, "CreatedAt", "UpdatedAt", "DeletedAt")))
	}
	assert.NoError(tx.Commit().Error)

	t.Log("Verifying upsert and find in single transaction")
	user1.NumFollowers++
	user2.NumFollowers--
	tx, err = ds.GetTransaction(ctx, user1)
	assert.NoError(err)
	defer rollbackTx(t, tx)
	txCtx = txFetcher.WithTransactionCtx(ctx, tx)
	for _, record := range []*AppUser{user1, user2} {
		rowsAffected, err := ds.Upsert(txCtx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)

		queryResult := &AppUser{Id: record.Id}
		err = ds.Find(txCtx, queryResult)
		assert.NoError(err)
		assert.True(cmp.Equal(record, queryResult, cmpopts.IgnoreFields(AppUser{}, "CreatedAt", "UpdatedAt", "DeletedAt")))
	}
	assert.NoError(tx.Commit().Error)

	t.Log("Verifying deletion and find in single transaction")
	tx, err = ds.GetTransaction(ctx, user1)
	assert.NoError(err)
	defer rollbackTx(t, tx)
	txCtx = txFetcher.WithTransactionCtx(ctx, tx)
	for _, record := range []*AppUser{user1, user2} {
		rowsAffected, err := ds.Delete(txCtx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)
		queryResult := AppUser{Id: record.Id}
		err = ds.Find(txCtx, &queryResult)
		assert.ErrorIs(err, ErrRecordNotFound)
		assert.True(queryResult.AreNonKeyFieldsEmpty())
	}
	assert.NoError(tx.Commit().Error)
}

func BenchmarkTxCrud(b *testing.B) {
	logger := logutils.GetLogger()
	logger.SetLevel(logrus.FatalLevel)
	logger.SetOutput(io.Discard)
	LOG = logger.WithField(logutils.COMP, logutils.SAAS_PERSISTENCE)

	var t testing.T
	ds, _ := SetupDataStore("BenchmarkTxCrud")
	defer ds.Reset()
	_, user1, user2 := SetupDbTables(ds)
	for n := 0; n < b.N; n++ {
		testTxCrud(&t, ds, CokeAdminCtx, user1, user2)
	}
}

func TestTxCrud(t *testing.T) {
	ds, _ := SetupDataStore("TestTxCrud")
	defer ds.Reset()
	_, user1, user2 := SetupDbTables(ds)
	testTxCrud(t, ds, CokeAdminCtx, user1, user2)
}

func TestSingleTxCrud(t *testing.T) {
	assert := assert.New(t)
	var err error

	ctx := CokeAdminCtx
	ds, _ := SetupDataStore("TestSingleTxCrud")
	defer ds.Reset()
	_, user1, user2 := SetupDbTables(ds)

	txFetcher := authorizer.SimpleTransactionFetcher{}
	tx, err := ds.GetTransaction(ctx, user1)
	assert.NoError(err)
	defer rollbackTx(t, tx)
	txCtx := txFetcher.WithTransactionCtx(ctx, tx)

	t.Log("Querying created records in same transaction")
	for _, record := range []*AppUser{user1, user2} {
		queryResult := &AppUser{Id: record.Id}
		err = ds.Find(txCtx, queryResult)
		assert.NoError(err)
		assert.True(cmp.Equal(record, queryResult, cmpopts.IgnoreFields(AppUser{}, "CreatedAt", "UpdatedAt", "DeletedAt")))
	}

	t.Log("Verifying update and find in same transaction")
	user1.Name = "Jeyhun G."
	user1.Email = "jeyhun111@mail.com"
	user1.EmailConfirmed = !user1.EmailConfirmed
	user1.NumFollowers++
	user2.Name = "Jahangir G."
	user2.Email = "jahangir111@mail.com"
	user2.EmailConfirmed = !user2.EmailConfirmed
	user2.NumFollowers--
	tx, err = ds.GetTransaction(ctx, user1)
	assert.NoError(err)
	defer rollbackTx(t, tx)
	txCtx = txFetcher.WithTransactionCtx(ctx, tx)
	for _, record := range []*AppUser{user1, user2} {
		rowsAffected, err := ds.Update(txCtx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)

		queryResult := &AppUser{Id: record.Id}
		err = ds.Find(txCtx, queryResult)
		assert.NoError(err)
		assert.True(cmp.Equal(record, queryResult, cmpopts.IgnoreFields(AppUser{}, "CreatedAt", "UpdatedAt", "DeletedAt")))
	}

	t.Log("Verifying upsert and find in same transaction")
	user1.NumFollowers++
	user2.NumFollowers--
	for _, record := range []*AppUser{user1, user2} {
		rowsAffected, err := ds.Upsert(txCtx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)

		queryResult := &AppUser{Id: record.Id}
		err = ds.Find(txCtx, queryResult)
		assert.NoError(err)
		assert.True(cmp.Equal(record, queryResult, cmpopts.IgnoreFields(AppUser{}, "CreatedAt", "UpdatedAt", "DeletedAt")))
	}

	t.Log("Verifying deletion and find in same transaction")
	for _, record := range []*AppUser{user1, user2} {
		rowsAffected, err := ds.Delete(txCtx, record)
		assert.NoError(err)
		assert.EqualValues(1, rowsAffected)
		queryResult := AppUser{Id: record.Id}
		err = ds.Find(txCtx, &queryResult)
		assert.ErrorIs(err, ErrRecordNotFound)
		assert.True(queryResult.AreNonKeyFieldsEmpty())
	}
	assert.NoError(tx.Commit().Error)
}
