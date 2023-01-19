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
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

const GLOBAL_DEFAULT_ORG_ID = "_GlobalDefaultOrg"

/*
Interface for everything auth.-related in DAL. Can be implemented by the users of DAL and passed to DAL using SetAuthorizer().
*/
type Authorizer interface {
	GetDefaultOrgAdminContext() context.Context
	GetAuthContext(orgId string, roles ...string) context.Context
	GetOrgFromContext(ctx context.Context) (string, error)
	GetMatchingDbRole(ctx context.Context, tableNames ...string) (DbRole, error)
	IsOperationAllowed(ctx context.Context, tableName string, record Record) error

	/*
		Method used to configure the authorizer. The method's arguments and implementation will be different for every authorizer.
	*/
	Configure(args ...interface{})
}

const (
	METADATA_KEY_ORGID            = "orgid"
	METADATA_KEY_ROLE             = "role"
	METADATA_ROLE_SERVICE_ADMIN   = "service_admin"
	METADATA_ROLE_SERVICE_AUDITOR = "service_auditor"
	METADATA_ROLE_ADMIN           = "admin"   // can be tenant_admin, *_admin
	METADATA_ROLE_AUDITOR         = "auditor" // can be tenant_auditor, *_auditor
)

type MetadataBasedAuthorizer struct{}

func (s MetadataBasedAuthorizer) GetOrgFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrorFetchingMetadataFromContext
	}

	logger.Debugf("Received metadata %+v from context", md)

	org := md[METADATA_KEY_ORGID]
	if len(org) == 0 {
		return "", ErrorMissingOrgId
	}

	return org[len(org)-1], nil
}

func (s MetadataBasedAuthorizer) GetMatchingDbRole(ctx context.Context, tableNames ...string) (DbRole, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrorFetchingMetadataFromContext
	}

	logger.Debugf("Received metadata %+v from context", md)

	role := md[METADATA_KEY_ROLE]
	if len(role) > 0 {
		switch role[len(role)-1] {
		case METADATA_ROLE_SERVICE_ADMIN:
			return WRITER, nil
		case METADATA_ROLE_SERVICE_AUDITOR:
			return READER, nil
		default:
			if strings.HasSuffix(role[len(role)-1], METADATA_ROLE_ADMIN) {
				return TENANT_WRITER, nil
			}
		}
	}
	return TENANT_READER, nil
}

func (s MetadataBasedAuthorizer) IsOperationAllowed(ctx context.Context, tableName string, record Record) error {
	// Get matching DB role
	dbRole, err := s.GetMatchingDbRole(ctx, tableName)
	if err != nil {
		return err
	}

	// If the DB role is tenant-specific (TENANT_READER or TENANT_WRITER) and the table is multi-tenant,
	// make sure that the record being inserted/modified/updated/deleted/queried belongs to the user's org.
	// If operation is SELECT but no specific tenant's data is being queried (e.g., FindAll() was called),
	// allow the operation to proceed
	if dbRole.IsTenantDbRole() && IsMultitenant(record, tableName) {
		orgId, err := s.GetOrgFromContext(ctx)
		// OrgId check required for Tenant DB roles only
		if err != nil {
			return err
		}

		orgIdCol := getField(ORG_ID_COLUMN_NAME, record)
		if orgIdCol != "" && orgIdCol != orgId {
			err = ErrOperationNotAllowed.WithValue("tenant", orgId).WithValue("orgIdCol", orgIdCol)
			return err
		}
	}
	return nil
}

func (s MetadataBasedAuthorizer) Configure(_ ...interface{}) {
	// TODO - Set "service role" to DB role mapping here
}

func (s MetadataBasedAuthorizer) GetAuthContext(orgId string, roles ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, orgId, METADATA_KEY_ROLE, roles[0]))
}

func (s MetadataBasedAuthorizer) GetDefaultOrgAdminContext() context.Context {
	return s.GetAuthContext(GLOBAL_DEFAULT_ORG_ID, METADATA_ROLE_ADMIN)
}
