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
	"testing"

	"github.com/bxcodec/faker/v4"
	"github.com/stretchr/testify/assert"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/pkgtest"
	"gorm.io/gorm"
)

func testSingleTableTransactions(t *testing.T) {
	t.Helper()
	assert := assert.New(t)

	g1 := &Group{}
	g2 := &Group{}
	_ = faker.FakeData(g1)
	_ = faker.FakeData(g2)

	t.Log("Getting DBTransaction for Groups")
	tx, err := TestDataStore.GetTransaction(ServiceAdminCtx, g1)
	assert.NoError(err)
	t.Log("Creating g1 and g2 in single transaction")
	err = tx.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(g1).Error; err != nil {
			return err
		}
		if err := tx.Create(g2).Error; err != nil {
			return err
		}

		return nil
	})
	tx.Commit()
	assert.NoError(err)
	assert.NoError(tx.Error)

	tx, err = TestDataStore.GetTransaction(ServiceAuditorCtx, g1)
	assert.NoError(err)
	t.Log("Listing all groups")
	groups := make([]Group, 0)
	err = tx.Transaction(func(tx *gorm.DB) error {
		if err := tx.Find(&groups).Error; err != nil {
			return err
		}
		return nil
	})
	tx.Commit()
	assert.NoError(err)
	assert.NoError(tx.Error)
	t.Log(groups)
	assert.Equal(2, len(groups))

	t.Log("Getting DBTransaction for Groups")
	tx, err = TestDataStore.GetTransaction(ServiceAuditorCtx, g1)
	assert.NoError(err)

	t.Log("Finding g1 and g2 in single transaction")
	f1, f2 := &Group{Id: g1.Id}, &Group{Id: g2.Id}
	err = tx.Transaction(func(tx *gorm.DB) error {
		if err := tx.First(f1).Error; err != nil {
			return err
		}

		if err := tx.First(f2).Error; err != nil {
			return err
		}
		return nil
	})
	t.Log("Verifying g1 and g2 are properly retrieved")
	tx.Commit()
	assert.NoError(err)
	assert.NoError(tx.Error)
	assert.Equal(g1, f1)
	assert.Equal(g2, f2)
	t.Log(g1, g2, f1, f2)

	t.Log("Deleting g1 and g2 in single transaction")
	tx, err = TestDataStore.GetTransaction(ServiceAdminCtx, g1)
	assert.NoError(err)
	err = tx.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(g1).Error; err != nil {
			return err
		}

		if err := tx.Delete(g2).Error; err != nil {
			return err
		}
		return nil
	})
	t.Log("Verifying g1 and g2 are deleted successfully")
	tx.Commit()
	assert.NoError(err)
	assert.NoError(tx.Error)
}

func testMultiTableTransactions(t *testing.T) {
	t.Helper()
	assert := assert.New(t)

	a1 := &App{}
	a2 := &AppUser{}
	_ = faker.FakeData(a1)
	_ = faker.FakeData(a2)
	a1.TenantId = COKE

	t.Log("Getting DBTransaction for creating App and AppUser")
	tx, err := TestDataStore.GetTransaction(CokeAdminCtx, a1, a2)
	assert.NoError(err)
	t.Log("Creating a1 and a2 in single transaction")
	err = tx.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(a1).Error; err != nil {
			return err
		}
		if err := tx.Create(a2).Error; err != nil {
			return err
		}

		return nil
	})
	tx.Commit()
	assert.NoError(err)
	assert.NoError(tx.Error)

	tx, err = TestDataStore.GetTransaction(CokeAuditorCtx, a1, a2)
	assert.NoError(err)
	t.Log("Finding app a1 and appuser a2 in single transaction")
	f1, f2 := &App{Id: a1.Id, TenantId: a1.TenantId}, &AppUser{Id: a2.Id}
	err = tx.Transaction(func(tx *gorm.DB) error {
		if err := tx.First(f1).Error; err != nil {
			return err
		}

		if err := tx.First(f2).Error; err != nil {
			return err
		}
		return nil
	})
	t.Log("Verifying a1 and a2 are properly retrieved")
	tx.Commit()
	assert.NoError(err)
	assert.NoError(tx.Error)
	assert.Equal(a1, f1)
	assert.Equal(a2, f2)
	t.Log(a1, a2)
	t.Log(f1, f2)

	t.Log("Deleting a1 and a2 in single transaction")
	tx, err = TestDataStore.GetTransaction(ServiceAdminCtx, a1)
	assert.NoError(err)
	err = tx.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(a1).Error; err != nil {
			return err
		}

		if err := tx.Delete(a2).Error; err != nil {
			return err
		}
		return nil
	})
	t.Log("Verifying a1 and a2 are deleted successfully")
	tx.Commit()
	assert.NoError(err)
	assert.NoError(tx.Error)
}
