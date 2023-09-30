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

package realization_store

import (
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/protostore"
)

type ProtobufWithMetadata struct {
	proto.Message
	protostore.Metadata
}

type OverallStatus struct {
	Id                string `gorm:"primaryKey"`
	OrgId             string `gorm:"primaryKey"`
	RealizationStatus Status
	AdditionalDetails string
	Revision          int64  `gorm:"column:resource_revision"`
	XTableName        string `gorm:"-"`
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (o *OverallStatus) TableName() string {
	return o.XTableName
}

type EnforcementStatus struct {
	Id                 string `gorm:"primaryKey"`
	OrgId              string `gorm:"primaryKey"`
	EnforcementPointId string `gorm:"primaryKey"`
	RealizationStatus  Status
	AdditionalDetails  string
	Revision           int64  `gorm:"column:resource_revision"`
	XTableName         string `gorm:"-"`
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func (e *EnforcementStatus) TableName() string {
	return e.XTableName
}

func GetOverallStatusTableName(msg proto.Message) string {
	return strings.Join([]string{"overall", "status", datastore.GetTableName(msg)}, "_")
}

func GetEnforcementStatusTableName(msg proto.Message) string {
	return strings.Join([]string{"enforcement", "status", datastore.GetTableName(msg)}, "_")
}

func GetModelEnforcementStatusRecordWithoutRevision(resource *ProtobufWithMetadata, orgId string) *EnforcementStatus {
	enforcementStatusRecord := EnforcementStatus{
		Id:         resource.Id,
		OrgId:      orgId,
		XTableName: GetEnforcementStatusTableName(resource.Message),
	}
	return &enforcementStatusRecord
}

func GetModelEnforcementStatusRecord(resource *ProtobufWithMetadata, orgId string) *EnforcementStatus {
	enforcementStatusRecord := EnforcementStatus{
		Id:         resource.Id,
		OrgId:      orgId,
		XTableName: GetEnforcementStatusTableName(resource.Message),
		Revision:   resource.Revision,
	}
	return &enforcementStatusRecord
}

func GetModelOverallStatusRecord(resource *ProtobufWithMetadata, orgId string) *OverallStatus {
	overallStatusRecord := OverallStatus{
		Id:         resource.Id,
		OrgId:      orgId,
		XTableName: GetOverallStatusTableName(resource.Message),
		Revision:   resource.Revision,
	}
	return &overallStatusRecord
}
