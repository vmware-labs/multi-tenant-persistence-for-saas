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

package dbrole_test

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
)

func randomizedDbRoles() dbrole.DbRoleSlice {
	a := dbrole.DbRoles()
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
	return a
}

func TestDbRoleOrdering(t *testing.T) {
	assert := assert.New(t)

	orderedRoles := dbrole.DbRoles()

	roles := randomizedDbRoles()
	sort.Sort(roles)
	assert.Equal(roles, orderedRoles)
}

func TestIsDbRoleTenantScoped(t *testing.T) {
	assert := assert.New(t)
	assert.False(dbrole.NO_ROLE.IsDbRoleTenantScoped())
	assert.False(dbrole.READER.IsDbRoleTenantScoped())
	assert.False(dbrole.WRITER.IsDbRoleTenantScoped())
	assert.True(dbrole.TENANT_INSTANCE_READER.IsDbRoleTenantScoped())
	assert.True(dbrole.TENANT_READER.IsDbRoleTenantScoped())
	assert.True(dbrole.TENANT_INSTANCE_WRITER.IsDbRoleTenantScoped())
	assert.True(dbrole.TENANT_WRITER.IsDbRoleTenantScoped())
}
