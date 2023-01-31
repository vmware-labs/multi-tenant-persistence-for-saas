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

package dbrole

import "strings"

// Database roles/users.
type DbRole string

const (
	// DB Roles.
	TENANT_READER DbRole = "tenant_reader"
	TENANT_WRITER DbRole = "tenant_writer"
	READER        DbRole = "reader"
	WRITER        DbRole = "writer"
)

type DbRoleSlice []DbRole // Needed for sorting records
func (a DbRoleSlice) Len() int {
	return len(a)
}

func (dbRole DbRole) IsDbRoleTenantScoped() bool {
	return dbRole == TENANT_READER || dbRole == TENANT_WRITER
}

/*
Returns true if the first role has fewer permissions than the second role, and true if the two roles are the same or
the second role has more permissions.
A reader role is always considered to have fewer permissions than a writer role.
and a tenant-specific reader/writer role is always considered to have fewer permissions than a non-tenant specific reader/writer role, respectively.

TENANT_READER < READER < TENANT_WRITER < WRITER.
*/
func (a DbRoleSlice) Less(i, j int) bool {
	var roleI, roleJ string = string(a[i]), string(a[j])
	switch {
	case roleI == roleJ:
		return true
	case strings.Contains(roleI, "reader") && strings.Contains(roleJ, "writer"):
		return true
	case strings.Contains(roleI, "writer") && strings.Contains(roleJ, "reader"):
		return false
	case strings.Contains(roleI, "reader") && strings.Contains(roleJ, "reader"):
		return DbRole(roleI) == TENANT_READER
	case strings.Contains(roleI, "writer") && strings.Contains(roleJ, "writer"):
		return DbRole(roleI) == TENANT_WRITER
	default:
		panic("Unable to compare " + roleI + " and " + roleJ)
	}
}

func (a DbRoleSlice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
