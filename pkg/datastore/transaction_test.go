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
	"gorm.io/gorm"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/protostore"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/test"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/test/pb"
)

func testSingleTableTransactions(t *testing.T, ds datastore.DataStore, ps protostore.ProtoStore) {
	t.Helper()
	assert := assert.New(t)

	g1 := &Group{}
	g2 := &Group{}
	_ = faker.FakeData(g1)
	_ = faker.FakeData(g2)
	g1.InstanceId = AMERICAS
	g2.InstanceId = AMERICAS

	t.Log("Getting DBTransaction for Groups")
	tx, err := ds.GetTransaction(AmericasCokeAdminCtx, g1)
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

	tx, err = ds.GetTransaction(AmericasCokeAdminCtx, g1)
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
	tx, err = ds.GetTransaction(AmericasCokeAuditorCtx, g1)
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
	tx, err = ds.GetTransaction(AmericasCokeAdminCtx, g1)
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

func testMultiTableTransactionsWithDifferentRoles(t *testing.T, ds datastore.DataStore, ps protostore.ProtoStore) {
	t.Helper()
	assert := assert.New(t)

	// Negative case as App has tenancy based roles and AppUser is non-tenancy based role
	a1 := &App{}
	a2 := &AppUser{}
	_ = faker.FakeData(a1)
	_ = faker.FakeData(a2)
	a1.TenantId = COKE

	t.Log("Getting DBTransaction for creating App and AppUser")
	tx, err := ds.GetTransaction(AmericasCokeAdminCtx, a1, a2)
	assert.NoError(err)
	t.Log("Creating App and AppUser in single transaction")
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
	assert.ErrorContains(err, "ERROR: permission denied for table application (SQLSTATE 42501)")
}

func testMultiProtoTransactionsWithDifferentRoles(t *testing.T, ds datastore.DataStore, ps protostore.ProtoStore) {
	t.Helper()
	assert := assert.New(t)

	c1 := &pb.Disk{}
	a2 := &App{}
	_ = faker.FakeData(c1)
	_ = faker.FakeData(a2)
	a1, err := ps.MsgToPersist(AmericasCokeAdminCtx, "a1", c1, protostore.Metadata{})
	assert.NoError(err)
	a2.TenantId = COKE
	t1 := datastore.GetTableName(a1)

	t.Log("Getting DBTransaction for creating pb.Disk and App")
	tx, err := ds.GetTransaction(AmericasCokeAdminCtx, a1, a2)
	assert.NoError(err)
	t.Log("Creating pb.Disk and App in single transaction")
	err = tx.Transaction(func(tx *gorm.DB) error {
		t.Logf("Creating %+v", a1)
		if err := tx.Table(t1).Create(a1).Error; err != nil {
			return err
		}
		t.Logf("Creating %+v", a2)
		if err := tx.Create(a2).Error; err != nil {
			return err
		}

		return nil
	})
	tx.Commit()
	assert.ErrorContains(err, "ERROR: permission denied for table application (SQLSTATE 42501)")
}
