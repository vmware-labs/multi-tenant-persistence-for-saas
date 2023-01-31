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
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/pkgtest"
)

/*
Checks if table name can be extracted from struct/slice of structs using utility methods.
*/
func TestGettingTableName(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("application", datastore.GetTableName(App{}))
	assert.Equal("application", datastore.GetTableName(&App{}))

	assert.Equal("app_users", datastore.GetTableName(AppUser{}))
	assert.Equal("app_users", datastore.GetTableName(&AppUser{}))

	assert.Equal("groups", datastore.GetTableName(Group{}))
	assert.Equal("groups", datastore.GetTableName(&Group{}))

	assert.Equal("app_users", datastore.GetTableNameFromSlice([]AppUser{}))
	assert.Equal("app_users", datastore.GetTableNameFromSlice(&[]AppUser{}))
	assert.Equal("app_users", datastore.GetTableNameFromSlice([]*AppUser{}))
	assert.Equal("app_users", datastore.GetTableNameFromSlice(&[]*AppUser{}))
}

func TestIsRevisioningSupported(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(true, datastore.IsRevisioningSupported("groups", Group{}))
	assert.Equal(true, datastore.IsRevisioningSupported("groups", &Group{}))
}
