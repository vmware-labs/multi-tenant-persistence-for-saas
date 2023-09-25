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

package authorizer_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/test"
)

// Checks that it's possible to extract org. ID from auth. context.
func testGettingOrgFromContext(t *testing.T, authorizer Authorizer) {
	t.Helper()
	assert := assert.New(t)

	// Negative test case - no auth. context in ctx
	_, err := authorizer.GetOrgFromContext(context.Background())
	assert.ErrorIs(err, ErrFetchingMetadata)

	// Negative test cases - missing org. ID
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ROLE, "admin"))
	_, err = authorizer.GetOrgFromContext(ctx)
	assert.ErrorIs(err, ErrMissingOrgId)

	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, PEPSI))
	orgId, err := authorizer.GetOrgFromContext(ctx)
	assert.NoError(err)
	assert.Equal(PEPSI, orgId)
}

func TestGettingOrgFromContextWithMetadataAuthorizer(t *testing.T) {
	testGettingOrgFromContext(t, &MetadataBasedAuthorizer{})
}

// Positive test case.
func TestGettingMatchingDbRoleWithMetadataBasedAuthorizer(t *testing.T) {
	assert := assert.New(t)

	authorizer := &MetadataBasedAuthorizer{}

	gCtx := authorizer.GetDefaultOrgAdminContext()
	dbRole, err := authorizer.GetMatchingDbRole(gCtx, "appUser", "app")
	assert.NoError(err)
	assert.Equal(dbrole.TENANT_WRITER, dbRole)

	aCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, PEPSI, METADATA_KEY_ROLE, METADATA_ROLE_SERVICE_ADMIN))
	dbRole, err = authorizer.GetMatchingDbRole(aCtx, "appUser", "app")
	assert.NoError(err)
	assert.Equal(dbrole.WRITER, dbRole)

	uCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, PEPSI, METADATA_KEY_ROLE, METADATA_ROLE_SERVICE_AUDITOR))
	dbRole, err = authorizer.GetMatchingDbRole(uCtx, "appUser", "app")
	assert.NoError(err)
	assert.Equal(dbrole.READER, dbRole)

	wCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, PEPSI, METADATA_KEY_ROLE, METADATA_ROLE_ADMIN))
	dbRole, err = authorizer.GetMatchingDbRole(wCtx, "appUser", "app")
	assert.NoError(err)
	assert.Equal(dbrole.TENANT_WRITER, dbRole)

	rCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ORGID, PEPSI, METADATA_KEY_ROLE, METADATA_ROLE_AUDITOR))
	dbRole, err = authorizer.GetMatchingDbRole(rCtx, "appUser", "app")
	assert.NoError(err)
	assert.Equal(dbrole.TENANT_READER, dbRole)
}

/*
Checks if updating role mapping in MetadataBasedAuthorizer works.
*/
func TestConfiguringMetadataBasedAuthorizer(t *testing.T) {
	assert := assert.New(t)

	authorizer := &MetadataBasedAuthorizer{}
	newRoleMapping := map[string]map[string]dbrole.DbRole{
		"app": {
			"SERVICE_ADMIN": dbrole.READER,
		},
	}
	authorizer.Configure("app", newRoleMapping["app"])

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ROLE, "SERVICE_ADMIN"))
	dbRole, err := authorizer.GetMatchingDbRole(ctx, "app")
	assert.NoError(err)
	assert.Equal(dbrole.READER, dbRole)

	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs(METADATA_KEY_ROLE, "SERVICE_OP"))
	dbRole, err = authorizer.GetMatchingDbRole(ctx, "app")
	assert.NoError(err)
	assert.Equal(dbrole.TENANT_READER, dbRole)
}
