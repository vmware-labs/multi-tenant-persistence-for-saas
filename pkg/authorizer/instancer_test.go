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

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/test"
)

func TestSimpleInstancer(t *testing.T) {
	assert := assert.New(t)
	instancer := authorizer.SimpleInstancer{}

	ctx := context.TODO()
	noId, noInstanceIdErr := instancer.GetInstanceId(ctx)
	assert.ErrorIs(ErrMissingInstanceId, noInstanceIdErr)
	assert.Equal("", noId)

	americasCtx := instancer.WithInstanceId(ctx, AMERICAS)
	americasInstanceId, err := instancer.GetInstanceId(americasCtx)
	assert.NoError(err)
	assert.Equal(AMERICAS, americasInstanceId)
}

func TestSimpleInstancerWithMetadataContext(t *testing.T) {
	assert := assert.New(t)
	instancer := authorizer.SimpleInstancer{}
	mdAuthorizer := authorizer.MetadataBasedAuthorizer{}

	mdCtx := mdAuthorizer.GetDefaultOrgAdminContext()
	noId, noInstanceIdErr := instancer.GetInstanceId(mdCtx)
	assert.ErrorIs(ErrMissingInstanceId, noInstanceIdErr)
	assert.Equal("", noId)

	americasMdCtx := instancer.WithInstanceId(mdCtx, AMERICAS)
	americasInstanceId, err := instancer.GetInstanceId(americasMdCtx)
	assert.NoError(err)
	assert.Equal(AMERICAS, americasInstanceId)
}
