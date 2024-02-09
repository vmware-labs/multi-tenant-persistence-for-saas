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

package authorizer

import (
	"context"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
)

type ContextLessAuthorizer struct {
	roleMapping map[string]map[string]dbrole.DbRole // Maps DB table to its service roles and matching DB roles
}

func (s *ContextLessAuthorizer) GetOrgFromContext(_ context.Context) (string, error) {
	return GLOBAL_DEFAULT_ORG_ID, nil
}

func (s *ContextLessAuthorizer) GetMatchingDbRole(_ context.Context, tableNames ...string) (dbrole.DbRole, error) {
	// Use roleMapping if configured
	if s.roleMapping != nil {
		allTableRoles := make([]dbrole.DbRole, 0)
		for _, tableName := range tableNames {
			dbRoles := make([]dbrole.DbRole, 0)
			for _, dbRole := range s.roleMapping[tableName] {
				dbRoles = append(dbRoles, dbRole)
			}
			if len(dbRoles) > 0 {
				allTableRoles = append(allTableRoles, dbrole.Max(dbRoles))
			}
		}
		if len(allTableRoles) > 0 {
			return dbrole.Min(allTableRoles), nil
		}
	}
	return dbrole.TENANT_READER, nil
}

func (s *ContextLessAuthorizer) Configure(tableName string, roleMapping map[string]dbrole.DbRole) {
	if s.roleMapping == nil {
		s.roleMapping = make(map[string]map[string]dbrole.DbRole)
	}
	s.roleMapping[tableName] = roleMapping
}

func (s *ContextLessAuthorizer) GetAuthContext(orgId string, roles ...string) context.Context {
	return nil
}

func (s *ContextLessAuthorizer) GetDefaultOrgAdminContext() context.Context {
	return nil
}
