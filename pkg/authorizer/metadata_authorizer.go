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

type MetadataBasedAuthorizer struct{}

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

	role := md[METADATA_KEY_ROLE]
	if len(role) > 0 {
		switch role[len(role)-1] {
		case METADATA_ROLE_SERVICE_ADMIN:
			return dbrole.WRITER, nil
		case METADATA_ROLE_SERVICE_AUDITOR:
			return dbrole.READER, nil
		default:
			if strings.HasSuffix(role[len(role)-1], METADATA_ROLE_ADMIN) {
				return dbrole.TENANT_WRITER, nil
			}
		}
	}
	return dbrole.TENANT_READER, nil
}

func (s MetadataBasedAuthorizer) Configure(_ string, _ map[string]dbrole.DbRole) {
	// TODO - Set "service role" to DB role mapping here"
	// Currently MetadataBasedAuthorizer, doesn't support service to DB role mapping
}

func (s MetadataBasedAuthorizer) GetAuthContext(orgId string, roles ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, orgId, METADATA_KEY_ROLE, roles[0]))
}

func (s *MetadataBasedAuthorizer) GetDefaultOrgAdminContext() context.Context {
	return s.GetAuthContext(GLOBAL_DEFAULT_ORG_ID, METADATA_ROLE_ADMIN)
}
