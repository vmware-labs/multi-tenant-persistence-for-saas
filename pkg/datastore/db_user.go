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

package datastore

import (
	"hash/fnv"
	"os"
	"strconv"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
)

// Specifications for database user.
type dbUserSpecs struct {
	username         dbrole.DbRole // Username/role name (in Postgres, users and roles are equivalent)
	password         string
	policyName       string
	commands         []string // Commands to be permitted in the policy. Could be SELECT, INSERT, UPDATE, DELETE
	existingRowsCond string   // SQL conditional expression to be checked for existing rows. Only those rows for which the condition is true will be visible.
	newRowsCond      string   // SQL conditional expression to be checked for rows being inserted or updated. Only those rows for which the condition is true will be inserted/updated
}

// Generates specifications of 4 DB users:
// - user with read-only access to his org
// - user with read & write access to his org
// - user with read-only access to all orgs
// - user with read & write access to all orgs.
func getDbUser(dbRole dbrole.DbRole) dbUserSpecs {
	for _, spec := range getAllDbUsers() {
		if spec.username == dbRole {
			return spec
		}
	}
	panic("Invalid DbRole " + string(dbRole))
}

/*
Generates specifications of 4 DB users:
- user with read-only access to his org
- user with read & write access to his org
- user with read-only access to all orgs
- user with read & write access to all orgs.
*/
func getDbUsers(tableName string) []dbUserSpecs {
	writer := dbUserSpecs{
		username:         dbrole.WRITER,
		commands:         []string{"SELECT", "INSERT", "UPDATE", "DELETE"},
		existingRowsCond: "true", // Allow access to all existing records
		newRowsCond:      "true", // Allow inserting or updating records
	}

	reader := dbUserSpecs{
		username:         dbrole.READER,
		commands:         []string{"SELECT"}, // Allow to perform SELECT on all records
		existingRowsCond: "true",             // Allow access to all existing records
		newRowsCond:      "false",            // Prevent inserting or updating records
	}

	tenancyCond := COLUMN_ORGID + " = current_setting('" + DbConfigOrgId + "')"
	tenantWriter := dbUserSpecs{
		username:         dbrole.TENANT_WRITER,
		commands:         []string{"SELECT", "INSERT", "UPDATE", "DELETE"},
		existingRowsCond: tenancyCond, // Allow access only to its tenant's records
		newRowsCond:      tenancyCond, // Allow inserting for or updating records of its own tenant
	}

	tenantReader := dbUserSpecs{
		username:         dbrole.TENANT_READER,
		commands:         []string{"SELECT"}, // Allow to perform SELECT on its tenant's records
		existingRowsCond: tenancyCond,        // Allow access only to its tenant's records
		newRowsCond:      "false",            // Prevent inserting or updating records
	}

	dbUsers := []dbUserSpecs{writer, reader, tenantWriter, tenantReader}
	for i := 0; i < len(dbUsers); i++ {
		dbUsers[i].password = getPassword(string(dbUsers[i].username))
		dbUsers[i].policyName = getRlsPolicyName(string(dbUsers[i].username), tableName)
	}
	return dbUsers
}

// Generates a password for a DB user by getting a hash of DB admin. password
// concatenated with a DB username and converting the hash to hex.
func getPassword(username string) string {
	h := fnv.New32a()
	h.Write([]byte(os.Getenv(DB_ADMIN_PASSWORD_ENV_VAR) + username))
	return strconv.FormatInt(int64(h.Sum32()), 16)
}

func getAllDbUsers() []dbUserSpecs {
	return getDbUsers("ANY")
}
