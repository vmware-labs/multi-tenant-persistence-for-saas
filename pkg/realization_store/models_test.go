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

package realization_store_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm/schema"

	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/realization_store"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/test/pb"
)

func TestStatusDbTableNames(t *testing.T) {
	assert := assert.New(t)

	cpuOverallStatusRecord := GetModelOverallStatusRecord(&ProtobufWithMetadata{Message: &pb.CPU{}}, "")
	memoryOverallStatusRecord := GetModelOverallStatusRecord(&ProtobufWithMetadata{Message: &pb.Memory{}}, "")
	assert.NotEqual(schema.Tabler(cpuOverallStatusRecord).TableName(), schema.Tabler(memoryOverallStatusRecord).TableName(),
		"Expected the names of overall status DB tables for 2 entities not to be the same")

	cpuEnforcementStatusRecord := GetModelEnforcementStatusRecord(&ProtobufWithMetadata{Message: &pb.CPU{}}, "")
	memoryEnforcementStatusRecord := GetModelEnforcementStatusRecord(&ProtobufWithMetadata{Message: &pb.Memory{}}, "")
	assert.NotEqual(schema.Tabler(cpuEnforcementStatusRecord).TableName(), schema.Tabler(memoryEnforcementStatusRecord).TableName(),
		"Expected the names of enforcement status DB tables for 2 entities not to be the same")
}
