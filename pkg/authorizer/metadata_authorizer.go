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
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/logutils"
)

const (
	GLOBAL_DEFAULT_ORG_ID = "_GlobalDefaultOrg"

	METADATA_KEY_ORGID            = "orgid"
	METADATA_KEY_ROLE             = "role"
	METADATA_ROLE_SERVICE_ADMIN   = "service_admin"
	METADATA_ROLE_SERVICE_AUDITOR = "service_auditor"
	METADATA_ROLE_ADMIN           = "admin"   // can be tenant_admin, *_admin
	METADATA_ROLE_AUDITOR         = "auditor" // can be tenant_auditor, *_auditor
)

type MetadataBasedAuthorizer struct {
	roleMapping map[string]map[string]dbrole.DbRole // Maps DB table to its service roles and matching DB roles
}

func (s *MetadataBasedAuthorizer) GetOrgFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrFetchingMetadata
	}

	orgIds := md[METADATA_KEY_ORGID]
	if len(orgIds) == 0 {
		return "", ErrMissingOrgId
	}

	return orgIds[len(orgIds)-1], nil
}

func (s *MetadataBasedAuthorizer) GetMatchingDbRole(ctx context.Context, tableNames ...string) (dbrole.DbRole, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return dbrole.NO_ROLE, ErrFetchingMetadata
	}

	// Returns the min role across tables and max within a table for given serviceRoles
	serviceRoles := md[METADATA_KEY_ROLE]
	if len(serviceRoles) > 0 {
		// Use roleMapping if configured
		if s.roleMapping != nil {
			allTableRoles := make([]dbrole.DbRole, 0)
			for _, tableName := range tableNames {
				dbRoles := make([]dbrole.DbRole, 0)
				for _, serviceRole := range serviceRoles {
					if rMapping, ok := s.roleMapping[tableName]; ok {
						if dbRole, ok := rMapping[serviceRole]; ok {
							dbRoles = append(dbRoles, dbRole)
						}
					}
				}
				if len(dbRoles) > 0 {
					allTableRoles = append(allTableRoles, dbrole.Max(dbRoles))
				} else {
					break
				}
			}
			if len(allTableRoles) > 0 {
				return dbrole.Min(allTableRoles), nil
			}
		}

		serviceRole := serviceRoles[len(serviceRoles)-1]
		switch serviceRole {
		case METADATA_ROLE_SERVICE_ADMIN:
			return dbrole.WRITER, nil
		case METADATA_ROLE_SERVICE_AUDITOR:
			return dbrole.READER, nil
		default:
			if strings.HasSuffix(serviceRole, METADATA_ROLE_ADMIN) {
				return dbrole.TENANT_WRITER, nil
			}
		}
	}
	return dbrole.TENANT_READER, nil
}

func (s *MetadataBasedAuthorizer) Configure(tableName string, roleMapping map[string]dbrole.DbRole) {
	if s.roleMapping == nil {
		TRACE("RoleMapping: setting to NEWMAP")
		s.roleMapping = make(map[string]map[string]dbrole.DbRole)
	}
	s.roleMapping[tableName] = roleMapping
	TRACE("RoleMapping: configured for table %s: %+v", tableName, s.roleMapping)
}

func (s *MetadataBasedAuthorizer) GetAuthContext(orgId string, roles ...string) context.Context {
	md := metadata.Pairs(METADATA_KEY_ORGID, orgId)
	for _, role := range roles {
		md.Append(METADATA_KEY_ROLE, role)
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func (s *MetadataBasedAuthorizer) GetDefaultOrgAdminContext() context.Context {
	return s.GetAuthContext(GLOBAL_DEFAULT_ORG_ID, METADATA_ROLE_ADMIN)
}
