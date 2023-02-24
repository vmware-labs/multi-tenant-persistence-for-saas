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
			pb.CPU{},
			&pb.CPU{},
			[]pb.CPU{},
			[]*pb.CPU{},
			&[]pb.CPU{},
			&[]*pb.CPU{},
		} {
			t.Logf("Testing GetTableName with %+v", datastore.TypeName(x))
			assert.Equal("processor", datastore.GetTableName(x))
		}
	}
}

func TestIsRevisioningSupported(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(true, datastore.IsRevisioningSupported("groups", Group{}))
	assert.Equal(true, datastore.IsRevisioningSupported("groups", &Group{}))
}
