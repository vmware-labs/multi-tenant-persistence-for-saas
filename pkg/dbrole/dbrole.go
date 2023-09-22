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

//	DAL uses 4 database roles/users to perform all operations,
//
//	  - `TENANT_READER` - has read access to its tenant's data
//	  - `READER` - has read access to all tenants' data
//	  - `TENANT_WRITER` - has read & write access to its tenant's data
//	  - `WRITER` - has read & write access to all tenants' data
//
// Corresponding *INSTANCE_* roles access is determined by the Instancer's configuration,
// allowing it to access records exclusively with a specific instance.
//
// DAL allows to map a user's service role to the DB role that will be used for
// that user. If a user has multiple service roles which map to several DB roles,
// the DB role with the most extensive privileges will be used (see `DbRoles()`
// for reference to ordered list of DbRoles.
package dbrole

import "sort"

// DbRole Database roles/users.
type DbRole string

const (
	// NO_ROLE DB Roles.
	NO_ROLE                DbRole = ""
	TENANT_INSTANCE_READER DbRole = "tenant_instance_reader"
	TENANT_READER          DbRole = "tenant_reader"
	INSTANCE_READER        DbRole = "instance_reader"
	READER                 DbRole = "reader"
	TENANT_INSTANCE_WRITER DbRole = "tenant_instance_writer"
	TENANT_WRITER          DbRole = "tenant_writer"
	INSTANCE_WRITER        DbRole = "instance_writer"
	WRITER                 DbRole = "writer"
	MAIN                   DbRole = "main"
)

// Returns *Ordered* slice of DbRoles.
// A reader role is always considered to have fewer permissions than a writer role.
// and a tenant-specific reader/writer role is always considered to have fewer permissions,
// than a non-tenant specific reader/writer role, respectively.
func DbRoles() DbRoleSlice {
	return DbRoleSlice{
		NO_ROLE,
		TENANT_INSTANCE_READER,
		TENANT_READER,
		INSTANCE_READER,
		READER,
		TENANT_INSTANCE_WRITER,
		TENANT_WRITER,
		INSTANCE_WRITER,
		WRITER,
		MAIN,
	}
}

func (dbRole DbRole) IsDbRoleTenantScoped() bool {
	return dbRole == TENANT_READER || dbRole == TENANT_WRITER || dbRole == TENANT_INSTANCE_READER || dbRole == TENANT_INSTANCE_WRITER
}

func (dbRole DbRole) IsDbRoleInstanceScoped() bool {
	return dbRole == INSTANCE_READER || dbRole == INSTANCE_WRITER || dbRole == TENANT_INSTANCE_READER || dbRole == TENANT_INSTANCE_WRITER
}

// Map roles to instancer based when Instancer is set.
func (dbRole DbRole) GetRoleWithInstancer() DbRole {
	switch dbRole {
	case READER:
		return INSTANCE_READER
	case WRITER:
		return INSTANCE_WRITER
	case TENANT_READER:
		return TENANT_INSTANCE_READER
	case TENANT_WRITER:
		return TENANT_INSTANCE_WRITER
	default:
		return dbRole
	}
}

// Map roles to non-tenancy when tenant column is not configured.
func (dbRole DbRole) GetRoleWithoutTenancy() DbRole {
	switch dbRole {
	case TENANT_READER:
		return READER
	case TENANT_WRITER:
		return WRITER
	case TENANT_INSTANCE_READER:
		return INSTANCE_READER
	case TENANT_INSTANCE_WRITER:
		return INSTANCE_WRITER
	default:
		return dbRole
	}
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

type DbRoleSlice []DbRole // Needed for sorting records
// Returns true if the first role has fewer permissions than the second
// role, and true if the two roles are the same or the second role has
// more permissions.
func (a DbRoleSlice) Less(i, j int) bool { return indexOf(a[i]) <= indexOf(a[j]) }

func (a DbRoleSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a DbRoleSlice) Len() int { return len(a) }

func Max(dbRoles []DbRole) DbRole {
	sort.Sort(DbRoleSlice(dbRoles))
	return dbRoles[len(dbRoles)-1]
}

func Min(dbRoles []DbRole) DbRole {
	sort.Sort(DbRoleSlice(dbRoles))
	return dbRoles[0]
}
