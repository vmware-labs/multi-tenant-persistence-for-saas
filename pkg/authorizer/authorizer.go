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

/*
Authorizer Interface defines the methods required for datastore to restrict access based on roles configured in context.
*/
type Authorizer interface {
	Tenancer
	Configure(tableName string, roleMapping map[string]dbrole.DbRole)
	GetAuthContext(orgId string, roles ...string) context.Context
	GetDefaultOrgAdminContext() context.Context
	GetMatchingDbRole(ctx context.Context, tableNames ...string) (dbrole.DbRole, error)
}

type Tenancer interface {
	GetOrgFromContext(ctx context.Context) (string, error)
}
