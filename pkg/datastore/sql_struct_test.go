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

	"github.com/stretchr/testify/assert"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/test"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/test/pb"
)

/*
Checks if table name can be extracted from struct/slice of structs using utility methods.
*/
func TestGettingTableName(t *testing.T) {
	assert := assert.New(t)

	{
		for _, x := range []interface{}{
			App{},
			&App{},
			[]App{},
			&[]App{},
			[]*App{},
			&[]*App{},
		} {
			t.Logf("Testing GetTableName with %+v", datastore.TypeName(x))
			assert.Equal("application", datastore.GetTableName(x))
		}
	}

	{
		for _, x := range []interface{}{
			AppUser{},
			&AppUser{},
			[]AppUser{},
			&[]AppUser{},
			[]*AppUser{},
			&[]*AppUser{},
		} {
			t.Logf("Testing GetTableName with %+v", datastore.TypeName(x))
			assert.Equal("app_users", datastore.GetTableName(x))
		}
	}

	{
		for _, x := range []interface{}{
			Group{},
			&Group{},
			[]Group{},
			&[]Group{},
			[]*Group{},
			&[]*Group{},
		} {
			t.Logf("Testing GetTableName with %+v", datastore.TypeName(x))
			assert.Equal("groups", datastore.GetTableName(x))
		}
	}

	{
		for _, x := range []interface{}{
			pb.Disk{},
			&pb.Disk{},
			[]pb.Disk{},
			[]*pb.Disk{},
			&[]pb.Disk{},
			&[]*pb.Disk{},
		} {
			t.Logf("Testing GetTableName with %+v", datastore.TypeName(x))
			assert.Equal("disks", datastore.GetTableName(x))
		}
	}
}

func TestGetTableName(t *testing.T) {
	assert := assert.New(t)
	type GroupMsg struct {
		Name           string
		ExpressionList []*string
		Next           *GroupMsg
		Children       []*GroupMsg
	}
	assert.Equal("group_msgs", datastore.GetTableName(GroupMsg{}))
}

func TestIsRevisioned(t *testing.T) {
	assert := assert.New(t)

	assert.True(datastore.IsRevisioned(Group{}, "groups"))
	assert.True(datastore.IsRevisioned(&Group{}, "groups"))
}

func TestSchemaParseFeatures(t *testing.T) {
	assert := assert.New(t)

	// No Features
	type S1 struct {
		Id   string
		Name string
	}

	// No Features
	type S2 struct {
		Id         string
		Name       string
		OrgId      string `gorm:"primaryKey;column:organization"`
		InstanceId string `gorm:"primaryKey;column:instance"`
		Revision   string `gorm:"-"`
	}

	// Features using field names
	type S3 struct {
		Id         string
		Name       string
		OrgId      string
		InstanceId string
		Revision   int64
	}

	// Features using golang tags
	type S4 struct {
		Id            string
		Name          string
		TenantId      string `gorm:"primaryKey;column:org_id"`
		DeploymentId  string `gorm:"primaryKey;column:instance_id"`
		UpdateVersion int64  `gorm:"column:revision"`
	}

	// Only Multi-tenancy using golang tags
	type S5 struct {
		Id         string
		Name       string
		TenantId   string `gorm:"primaryKey;column:org_id"`
		InstanceId string `gorm:"-"`
		Revision   int64
	}

	// Only Multi-instances using field names
	type S6 struct {
		Id         string
		Name       string
		TenantId   string `gorm:"-"`
		InstanceId string
	}

	assert.False(datastore.IsMultiTenanted(S1{}, "S1"))
	assert.False(datastore.IsMultiTenanted(S2{}, "S2"))
	assert.True(datastore.IsMultiTenanted(S3{}, "S3"))
	assert.True(datastore.IsMultiTenanted(S4{}, "S4"))
	assert.True(datastore.IsMultiTenanted(S5{}, "S5"))
	assert.False(datastore.IsMultiTenanted(S6{}, "S6"))

	assert.False(datastore.IsRevisioned(S1{}, "S1"))
	assert.False(datastore.IsRevisioned(S2{}, "S2"))
	assert.True(datastore.IsRevisioned(S3{}, "S3"))
	assert.True(datastore.IsRevisioned(S4{}, "S4"))
	assert.True(datastore.IsRevisioned(S5{}, "S5"))
	assert.False(datastore.IsRevisioned(S6{}, "S6"))

	assert.False(datastore.IsMultiInstanced(S1{}, "S1", true))
	assert.False(datastore.IsMultiInstanced(S2{}, "S2", true))
	assert.True(datastore.IsMultiInstanced(S3{}, "S3", true))
	assert.True(datastore.IsMultiInstanced(S4{}, "S4", true))
	assert.False(datastore.IsMultiInstanced(S5{}, "S5", true))
	assert.True(datastore.IsMultiInstanced(S6{}, "S6", true))

	assert.False(datastore.IsMultiInstanced(S1{}, "S1", false))
	assert.False(datastore.IsMultiInstanced(S2{}, "S2", false))
	assert.False(datastore.IsMultiInstanced(S3{}, "S3", false))
	assert.False(datastore.IsMultiInstanced(S4{}, "S4", false))
	assert.False(datastore.IsMultiInstanced(S5{}, "S5", false))
	assert.False(datastore.IsMultiInstanced(S6{}, "S6", false))

	assert.False(datastore.IsRowLevelSecurityRequired(S1{}, "S1", true))
	assert.False(datastore.IsRowLevelSecurityRequired(S2{}, "S2", true))
	assert.True(datastore.IsRowLevelSecurityRequired(S3{}, "S3", true))
	assert.True(datastore.IsRowLevelSecurityRequired(S4{}, "S4", true))
	assert.True(datastore.IsRowLevelSecurityRequired(S5{}, "S5", true))
	assert.True(datastore.IsRowLevelSecurityRequired(S6{}, "S6", true))

	assert.False(datastore.IsRowLevelSecurityRequired(S1{}, "S1", false))
	assert.False(datastore.IsRowLevelSecurityRequired(S2{}, "S2", false))
	assert.True(datastore.IsRowLevelSecurityRequired(S3{}, "S3", false))
	assert.True(datastore.IsRowLevelSecurityRequired(S4{}, "S4", false))
	assert.True(datastore.IsRowLevelSecurityRequired(S5{}, "S5", false))
	assert.False(datastore.IsRowLevelSecurityRequired(S6{}, "S6", false))
}
