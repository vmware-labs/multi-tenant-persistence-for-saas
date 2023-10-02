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

package test

import (
	"encoding/json"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/protostore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/realization_store"
)

const (
	// Organizations.
	COKE  = "Coke"
	PEPSI = "Pepsi"

	// Instances.
	AMERICAS = "Americas"
	EUROPE   = "Europe"
)

// Service roles for test cases.
const (
	TENANT_AUDITOR  = "tenant_auditor"
	TENANT_ADMIN    = "tenant_admin"
	SERVICE_AUDITOR = "service_auditor"
	SERVICE_ADMIN   = "service_admin"
)

var (
	RANDOM_ID              string = uuid.New().String()
	TestMetadataAuthorizer        = &authorizer.MetadataBasedAuthorizer{}
	TestInstancer                 = &authorizer.SimpleInstancer{}
	ServiceAdminCtx               = TestMetadataAuthorizer.GetAuthContext("", SERVICE_ADMIN)
	ServiceAuditorCtx             = TestMetadataAuthorizer.GetAuthContext("", SERVICE_AUDITOR)
	CokeAdminCtx                  = TestMetadataAuthorizer.GetAuthContext(COKE, TENANT_ADMIN)
	CokeAuditorCtx                = TestMetadataAuthorizer.GetAuthContext(COKE, TENANT_AUDITOR)
	PepsiAdminCtx                 = TestMetadataAuthorizer.GetAuthContext(PEPSI, TENANT_ADMIN)
	PepsiAuditorCtx               = TestMetadataAuthorizer.GetAuthContext(PEPSI, TENANT_AUDITOR)

	AmericasCokeAdminCtx   = TestInstancer.WithInstanceId(CokeAdminCtx, AMERICAS)
	AmericasCokeAuditorCtx = TestInstancer.WithInstanceId(CokeAuditorCtx, AMERICAS)
	AmericasPepsiAdminCtx  = TestInstancer.WithInstanceId(PepsiAdminCtx, AMERICAS)
	EuropeCokeAdminCtx     = TestInstancer.WithInstanceId(CokeAdminCtx, EUROPE)
	EuropeCokeAuditorCtx   = TestInstancer.WithInstanceId(CokeAuditorCtx, EUROPE)
)

type AppUser struct {
	Id             string `gorm:"primaryKey;column:user_id"`
	Name           string
	Email          string
	EmailConfirmed bool
	NumFollowing   int32
	NumFollowers   int64
	AppId          string
	Msg            []byte
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeletedAt      gorm.DeletedAt
}

type AppUserSlice []AppUser // Needed for sorting
func (a AppUserSlice) Len() int {
	return len(a)
}

func (a AppUserSlice) Less(x, y int) bool {
	return a[x].Id < a[y].Id
}
func (a AppUserSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

type App struct {
	Id        string `gorm:"primaryKey;column:application_id"`
	Name      string
	TenantId  string `gorm:"primaryKey;column:org_id"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt
}

func (a App) TableName() string {
	return "application"
}

func (a AppUser) AreNonKeyFieldsEmpty() bool {
	a.Id = ""
	return cmp.Equal(a, AppUser{})
}

func (a AppUser) String() string {
	if bytes, err := json.Marshal(a); err != nil {
		return "{}"
	} else {
		return string(bytes)
	}
}

func (a App) AreNonKeyFieldsEmpty() bool {
	a.Id = ""
	a.TenantId = ""
	return cmp.Equal(a, App{})
}

func (a App) String() string {
	if bytes, err := json.Marshal(a); err != nil {
		return "{}"
	} else {
		return string(bytes)
	}
}

type Group struct {
	Id         string `gorm:"primaryKey"`
	Name       string
	Revision   int
	InstanceId string `gorm:"primaryKey"`
}

func (g Group) String() string {
	if bytes, err := json.Marshal(g); err != nil {
		return "{}"
	} else {
		return string(bytes)
	}
}

// TODO: Get rid of global variables.
var (
	LOG     *logrus.Entry
	DS      datastore.DataStore
	PS      protostore.ProtoStore
	RS      realization_store.IRealizationStore
	NO_ROLE dbrole.DbRole

	TestAuthorizer authorizer.Authorizer
)

func InitTestData(dbName string) {
	InitLog()
	var err error
	NO_ROLE = dbrole.DbRole("")
	InitMetadataAuthorizer()
	InitAuthContexts()

	LOG.Info("Initializing data/proto/realization stores")
	DS, err = datastore.FromEnvWithDB(LOG, TestAuthorizer, nil, dbName)
	if err != nil {
		LOG.Fatalf("Failed to get default datastore: %e", err)
	}
	PS = protostore.GetProtoStore(LOG, DS)
	RS = realization_store.GetRealizationStore(DS, PS, LOG)
	LOG.Info("Initialized data/proto/realization stores successfully")
}

func InitLog() *logrus.Logger {
	logger := logrus.New()
	LOG = logger.WithFields(logrus.Fields{
		"service": "gotest",
	})
	return logger
}

func InitMetadataAuthorizer() {
	TestAuthorizer = &authorizer.MetadataBasedAuthorizer{}
}

func InitAuthContexts() {
	ServiceAdminCtx = TestAuthorizer.GetAuthContext("", SERVICE_ADMIN)
	CokeAdminCtx = TestAuthorizer.GetAuthContext(COKE, TENANT_ADMIN)
	CokeAuditorCtx = TestAuthorizer.GetAuthContext(COKE, TENANT_AUDITOR)
	PepsiAdminCtx = TestAuthorizer.GetAuthContext(PEPSI, TENANT_ADMIN)
	PepsiAuditorCtx = TestAuthorizer.GetAuthContext(PEPSI, TENANT_AUDITOR)
}
