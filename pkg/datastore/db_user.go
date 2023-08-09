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
	"encoding/json"
	"hash/fnv"
	"os"
	"strconv"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
)

// Specifications for database user.
type dbUserSpec struct {
	username         dbrole.DbRole // Username/role name (in Postgres, users and roles are equivalent)
	password         string
	policyName       string
	commands         []string // Commands to be permitted in the policy. Could be SELECT, INSERT, UPDATE, DELETE
	existingRowsCond string   // SQL conditional expression to be checked for existing rows. Only those rows for which the condition is true will be visible.
	newRowsCond      string   // SQL conditional expression to be checked for rows being inserted or updated. Only those rows for which the condition is true will be inserted/updated
}

func (m dbUserSpec) MarshalJSON() ([]byte, error) {
	data := struct {
		Username         dbrole.DbRole
		Password         string
		PolicyName       string
		Commands         []string
		ExistingRowsCond string
		NewRowsCond      string
	}{
		Username:         m.username,
		Password:         "*****",
		PolicyName:       m.policyName,
		Commands:         m.commands,
		ExistingRowsCond: m.existingRowsCond,
		NewRowsCond:      m.newRowsCond,
	}

	// Marshal the anonymous struct to JSON
	jsonData, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		return nil, err
	}

	return jsonData, nil
}

func (spec dbUserSpec) String() string {
	jsonBytes, err := spec.MarshalJSON()
	if err != nil {
		return "{}"
	}

	return string(jsonBytes)
}

func getDbUser(dbRole dbrole.DbRole) dbUserSpec {
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
All the users have additional conditions to restrict access to records
belonging to specific instance, if withInstanceIdCheck is set.
*/
func getDbUsers(tableName string, withTenantIdCheck, withInstanceIdCheck bool) []dbUserSpec {
	cond := "true"
	if withInstanceIdCheck {
		cond = COLUMN_INSTANCEID + " = current_setting('" + DbConfigInstanceId + "')"
	}

	writer := dbUserSpec{
		username:         dbrole.WRITER,
		commands:         []string{"SELECT", "INSERT", "UPDATE", "DELETE"},
		existingRowsCond: cond, // Allow access to all existing records
		newRowsCond:      cond, // Allow inserting or updating records
	}

	reader := dbUserSpec{
		username:         dbrole.READER,
		commands:         []string{"SELECT"}, // Allow to perform SELECT on all records
		existingRowsCond: cond,               // Allow access to all existing records
		newRowsCond:      "false",            // Prevent inserting or updating records
	}

	rlsCond := "true"
	if withTenantIdCheck && withInstanceIdCheck {
		rlsCond = COLUMN_ORGID + " = current_setting('" + DbConfigOrgId + "')"
		rlsCond += " AND " + COLUMN_INSTANCEID + " = current_setting('" + DbConfigInstanceId + "')"
	}
	if !withTenantIdCheck && withInstanceIdCheck {
		rlsCond = COLUMN_INSTANCEID + " = current_setting('" + DbConfigInstanceId + "')"
	}
	if withTenantIdCheck && !withInstanceIdCheck {
		rlsCond = COLUMN_ORGID + " = current_setting('" + DbConfigOrgId + "')"
	}

	tenantWriter := dbUserSpec{
		username:         dbrole.TENANT_WRITER,
		commands:         []string{"SELECT", "INSERT", "UPDATE", "DELETE"},
		existingRowsCond: rlsCond, // Allow access only to its tenant's records
		newRowsCond:      rlsCond, // Allow inserting for or updating records of its own tenant
	}

	tenantReader := dbUserSpec{
		username:         dbrole.TENANT_READER,
		commands:         []string{"SELECT"}, // Allow to perform SELECT on its tenant's records
		existingRowsCond: rlsCond,            // Allow access only to its tenant's records
		newRowsCond:      "false",            // Prevent inserting or updating records
	}

	dbUsers := []dbUserSpec{writer, reader, tenantWriter, tenantReader}
	for i := 0; i < len(dbUsers); i++ {
		dbUsers[i].password = getPassword(string(dbUsers[i].username))
		dbUsers[i].policyName = getRlsPolicyName(string(dbUsers[i].username), tableName)
	}
	TRACE("Returning DB user specs for table %q:\n\twithTenantIdCheck - %t\n\twithInstanceIdCheck -  %t\n\tdbUsers - %+v\n",
		tableName, withTenantIdCheck, withInstanceIdCheck, dbUsers)
	return dbUsers
}

// Generates a password for a DB user by getting a hash of DB admin. password
// concatenated with a DB username and converting the hash to hex.
func getPassword(username string) string {
	h := fnv.New32a()
	h.Write([]byte(os.Getenv(DB_ADMIN_PASSWORD_ENV_VAR) + username))
	return strconv.FormatInt(int64(h.Sum32()), 16)
}

func getAllDbUsers() []dbUserSpec {
	return getDbUsers("ANY", false, false) // Used for creating users only
}
