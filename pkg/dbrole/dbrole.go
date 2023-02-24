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

//	 DAL uses 4 database roles/users to perform all operations,
//
//		- `TENANT_READER` - has read access to its tenant's data
//		- `READER` - has read access to all tenants' data
//		- `TENANT_WRITER` - has read & write access to its tenant's data
//		- `WRITER` - has read & write access to all tenants' data
//
//	 DAL allows to map a user's service role to the DB role that will be used for
//	 that user. If a user has multiple service roles which map to several DB roles,
//	 the DB role with the most extensive privileges will be used (see `DbRoles()`
//	 for reference to ordered list of DbRoles.
package dbrole

// DbRole Database roles/users.
type DbRole string

const (
	// NO_ROLE DB Roles.
	NO_ROLE       DbRole = ""
	TENANT_READER DbRole = "tenant_reader"
	READER        DbRole = "reader"
	TENANT_WRITER DbRole = "tenant_writer"
	WRITER        DbRole = "writer"
	MAIN          DbRole = "main"
)

// Returns *Ordered* slice of DbRoles.
// A reader role is always considered to have fewer permissions than a writer role.
// and a tenant-specific reader/writer role is always considered to have fewer permissions,
// than a non-tenant specific reader/writer role, respectively.
// NO_ROLE < TENANT_READER < READER < TENANT_WRITER < WRITER.
func DbRoles() DbRoleSlice {
	return DbRoleSlice{
		NO_ROLE,
		TENANT_READER,
		READER,
		TENANT_WRITER,
		WRITER,
		MAIN,
	}
}

type DbRoleSlice []DbRole // Needed for sorting records
func (a DbRoleSlice) Len() int {
	return len(a)
}

func (dbRole DbRole) IsDbRoleTenantScoped() bool {
	return dbRole == TENANT_READER || dbRole == TENANT_WRITER
}

func indexOf(dbRole DbRole) int {
	orderedDbRoles := DbRoles()
	for idx, role := range orderedDbRoles {
		if dbRole == role {
			return idx
		}
	}
	return -1
}

// Returns true if the first role has fewer permissions than the second
// role, and true if the two roles are the same or the second role has
// more permissions.
func (a DbRoleSlice) Less(i, j int) bool {
	return indexOf(a[i]) <= indexOf(a[j])
}

func (a DbRoleSlice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
