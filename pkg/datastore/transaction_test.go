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
	"github.com/bxcodec/faker/v4/pkg/options"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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

func testMultiTableTransactions(t *testing.T, ds datastore.DataStore, ps protostore.ProtoStore) {
	t.Helper()
	assert := assert.New(t)

	a1 := &App{}
	a2 := &AppUser{}
	_ = faker.FakeData(a1, options.WithFieldsToIgnore("CreatedAt", "UpdatedAt", "DeletedAt"))
	_ = faker.FakeData(a2, options.WithFieldsToIgnore("CreatedAt", "UpdatedAt", "DeletedAt"))

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
	assert.NoError(err)
	assert.NoError(tx.Error)

	tx, err = ds.GetTransaction(AmericasCokeAuditorCtx, a1, a2)
	assert.NoError(err)
	t.Log("Finding App and Appuser in single transaction")
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
	t.Log("Verifying App and AppUser are properly retrieved")
	tx.Commit()
	assert.NoError(err)
	assert.NoError(tx.Error)
	assert.True(cmp.Equal(a1, f1, cmpopts.IgnoreFields(App{}, "CreatedAt", "UpdatedAt", "DeletedAt")))
	assert.True(cmp.Equal(a2, f2, cmpopts.IgnoreFields(App{}, "CreatedAt", "UpdatedAt", "DeletedAt")))
	t.Log(a1, a2)
	t.Log(f1, f2)

	t.Log("Deleting App and AppUser in single transaction")
	tx, err = ds.GetTransaction(ServiceAdminCtx, a1)
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
	t.Log("Verifying App and AppUser are deleted successfully")
	tx.Commit()
	assert.NoError(err)
	assert.NoError(tx.Error)
}

func testMultiProtoTransactions(t *testing.T, ds datastore.DataStore, ps protostore.ProtoStore) {
	t.Helper()
	assert := assert.New(t)

	c1 := &pb.Disk{}
	a2 := &App{}
	_ = faker.FakeData(c1)
	_ = faker.FakeData(a2, options.WithFieldsToIgnore("CreatedAt", "UpdatedAt", "DeletedAt"))
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
	assert.NoError(err)
	assert.NoError(tx.Error)

	tx, err = ds.GetTransaction(AmericasCokeAuditorCtx, a1, a2)
	assert.NoError(err)
	t.Log("Finding pb.Disk and App in single transaction")
	f1, err := ps.MsgToFilter(AmericasCokeAuditorCtx, "a1", c1)
	assert.NoError(err)
	f2 := &App{Id: a2.Id, TenantId: a2.TenantId}
	err = tx.Transaction(func(tx *gorm.DB) error {
		t.Logf("Finding %+v", f1)
		if err := tx.Table(t1).First(f1).Error; err != nil {
			return err
		}

		t.Logf("Finding %+v", f2)
		if err := tx.First(f2).Error; err != nil {
			return err
		}
		return nil
	})
	t.Log("Verifying pb.Disk and App are properly retrieved")
	tx.Commit()
	assert.NoError(err)
	assert.NoError(tx.Error)
	assert.Equal(a1.Msg, f1.Msg)
	assert.True(cmp.Equal(a2, f2, cmpopts.IgnoreFields(App{}, "CreatedAt", "UpdatedAt", "DeletedAt")))
	t.Log(a1, a2)
	t.Log(f1, f2)

	t.Log("Soft deleting pb.Disk and App in single transaction")
	tx, err = ds.GetTransaction(AmericasCokeAdminCtx, a1)
	assert.NoError(err)
	err = tx.Transaction(func(tx *gorm.DB) error {
		t.Logf("Soft deleting %+v", a1)
		if err := tx.Table(t1).Delete(a1).Error; err != nil {
			return err
		}

		t.Logf("Soft deleting %+v", a2)
		if err := tx.Delete(a2).Error; err != nil {
			return err
		}
		return nil
	})
	t.Log("Verifying pb.Disk and App are deleted successfully")
	tx.Commit()
	assert.NoError(err)
	assert.NoError(tx.Error)

	t.Log("Finding pb.Disk and App after soft delete should not return anything")
	tx, err = ds.GetTransaction(AmericasCokeAdminCtx, a1)
	assert.NoError(err)
	f1, err = ps.MsgToFilter(AmericasCokeAuditorCtx, "a1", c1)
	assert.NoError(err)
	f2 = &App{Id: a2.Id, TenantId: a2.TenantId}
	err = tx.Transaction(func(tx *gorm.DB) error {
		t.Logf("Finding %+v", f1)
		x := tx.Table(t1).First(f1)
		assert.EqualValues(0, x.RowsAffected, x)

		t.Logf("Finding %+v", f2)
		y := tx.First(f2)
		assert.EqualValues(0, y.RowsAffected, y)
		return nil
	})
	tx.Commit()
	assert.NoError(err)

	t.Log("Verifying pb.Disk and App can be retrieved after soft delete ...")
	tx, err = ds.GetTransaction(AmericasCokeAdminCtx, a1)
	assert.NoError(err)
	f1, err = ps.MsgToFilter(AmericasCokeAuditorCtx, "a1", c1)
	assert.NoError(err)
	f2 = &App{Id: a2.Id, TenantId: a2.TenantId}
	err = tx.Transaction(func(tx *gorm.DB) error {
		t.Logf("Finding %+v after soft-delete", f1)
		x := tx.Table(t1).Unscoped().First(f1)
		assert.EqualValues(1, x.RowsAffected, x)

		t.Logf("Finding %+v", f2)
		y := tx.Unscoped().First(f2)
		assert.EqualValues(1, y.RowsAffected, y)
		return nil
	})
	tx.Commit()
	assert.NoError(err)
	t.Log("Verifying pb.Disk can be retrieved after soft delete succeeded")

	t.Log("Purging pb.Disk after soft delete ...")
	tx, err = ds.GetTransaction(AmericasCokeAdminCtx, a1)
	assert.NoError(err)
	f1, err = ps.MsgToFilter(AmericasCokeAuditorCtx, "a1", c1)
	assert.NoError(err)
	f2 = &App{Id: a2.Id, TenantId: a2.TenantId}
	err = tx.Transaction(func(tx *gorm.DB) error {
		t.Logf("Purging %+v after soft-delete", f1)
		x := tx.Table(t1).Unscoped().Delete(f1)
		assert.EqualValues(1, x.RowsAffected, x)

		t.Logf("Purging %+v after soft-delete", f2)
		y := tx.Unscoped().Delete(f2)
		assert.EqualValues(1, y.RowsAffected, y)
		return nil
	})
	tx.Commit()
	assert.NoError(err)
	t.Log("Purging pb.Disk after soft delete succeeded")
}
