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
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

// Checks that it's possible to extract org. ID from auth. context.
func testGettingOrgFromContext(t *testing.T, authorizer Authorizer) {
	t.Helper()
	assert := assert.New(t)

	// Negative test case - no auth. context in ctx
	_, err := authorizer.GetOrgFromContext(context.Background())
	assert.ErrorIs(err, ErrorFetchingMetadataFromContext)

	// Negative test cases - missing org. ID
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ROLE, "admin"))
	_, err = authorizer.GetOrgFromContext(ctx)
	assert.ErrorIs(err, ErrorMissingOrgId)

	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, PEPSI))
	orgId, err := authorizer.GetOrgFromContext(ctx)
	assert.Nil(err)
	assert.Equal(PEPSI, orgId)
}

func TestGettingOrgFromContextWithMetadataAuthorizer(t *testing.T) {
	testGettingOrgFromContext(t, MetadataBasedAuthorizer{})
}

// Positive test case.
func TestGettingMatchingDbRoleWithMetadataBasedAuthorizer(t *testing.T) {
	assert := assert.New(t)

	authorizer := MetadataBasedAuthorizer{}

	wCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, PEPSI, METADATA_KEY_ROLE, METADATA_ROLE_ADMIN))
	dbRole, err := authorizer.GetMatchingDbRole(wCtx, "appUser", "app")
	assert.Nil(err)
	assert.Equal(TENANT_WRITER, dbRole)

	rCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, PEPSI, METADATA_KEY_ROLE, METADATA_ROLE_AUDITOR))
	dbRole, err = authorizer.GetMatchingDbRole(rCtx, "appUser", "app")
	assert.Nil(err)
	assert.Equal(TENANT_READER, dbRole)
}

/*
Positive test case.
Checks that MetadataBasedAuthorizer permits access to other tenants' data for READER & WRITER roles
and does not permit access to other tenants' data for TENANT_READER & TENANT_WRITER roles.
*/
func TestAllowingOperationsWithMetadataBasedAuthorizer(t *testing.T) {
	assert := assert.New(t)

	authorizer := MetadataBasedAuthorizer{}
	serviceAdminCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, PEPSI, METADATA_KEY_ROLE, METADATA_ROLE_ADMIN))
	tenantAuditorCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, PEPSI, METADATA_KEY_ROLE, METADATA_ROLE_AUDITOR))

	someApp := app{
		Id:    "001",
		OrgId: COKE, // Another tenant's record
	}

	// You should not be able to access other tenants' data with TENANT_READER role
	err := authorizer.IsOperationAllowed(tenantAuditorCtx, "app", someApp)
	assert.ErrorIs(err, ErrOperationNotAllowed)

	// You should be able to access other tenants' data with TENANT_WRITER role
	err = authorizer.IsOperationAllowed(serviceAdminCtx, "app", someApp)
	assert.ErrorIs(err, ErrOperationNotAllowed)

	// You should always be able to perform a SELECT operation if you're not adding a filter with another tenant's org. ID
	err = authorizer.IsOperationAllowed(serviceAdminCtx, "app", app{})
	assert.Nil(err)
}

/*
Checks if updating role mapping in MetadataBasedAuthorizer works.
*/
func TestConfiguringMetadataBasedAuthorizer(t *testing.T) {
	authorizer := MetadataBasedAuthorizer{}

	newRoleMapping := map[string]map[string]DbRole{
		"app": {
			"SERVICE_ADMIN": WRITER,
		},
	}

	authorizer.Configure("app", newRoleMapping["app"])
}
