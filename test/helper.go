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
	"log"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
)

func DropAllTables(ds datastore.DataStore) {
	if err := ds.TestHelper().DropTables(&App{}, &AppUser{}, &Group{}); err != nil {
		log.Fatalf("Failed to drop DB tables: %+v", err)
	}
}

func RecreateAllTables(ds datastore.DataStore) {
	DropAllTables(ds)
	roleMapping := map[string]dbrole.DbRole{
		TENANT_AUDITOR:  dbrole.TENANT_READER,
		TENANT_ADMIN:    dbrole.TENANT_WRITER,
		SERVICE_AUDITOR: dbrole.READER,
		SERVICE_ADMIN:   dbrole.WRITER,
	}

	for _, record := range []datastore.Record{&App{}, &AppUser{}, &Group{}} {
		if err := ds.Register(ServiceAdminCtx, roleMapping, record); err != nil {
			log.Fatalf("Failed to create DB tables: %+v", err)
		}
	}
}

func SetupDbTables(ds datastore.DataStore) (*App, *AppUser, *AppUser) {
	RecreateAllTables(ds)
	myCokeApp, user1, user2 := PrepareInput()
	for _, record := range []datastore.Record{myCokeApp, user1, user2} {
		if _, err := ds.Insert(CokeAdminCtx, record); err != nil {
			log.Fatalf("Failed to prepare input: %+v", err)
		}
	}
	return myCokeApp, user1, user2
}

func PrepareInput() (*App, *AppUser, *AppUser) {
	myCokeApp := App{
		Id:       "Coke-" + RANDOM_ID,
		Name:     "Cool_app",
		TenantId: COKE,
	}

	user1 := AppUser{
		Id:             "User1-" + RANDOM_ID,
		Name:           "Jeyhun",
		Email:          "jeyhun@mail.com",
		EmailConfirmed: true,
		NumFollowing:   2147483647,          // int32 type
		NumFollowers:   9223372036854775807, // int64 type
		AppId:          myCokeApp.Id,
		Msg:            []byte("msg1234"),
	}

	user2 := AppUser{
		Id:             "User2-" + RANDOM_ID,
		Name:           "Jahangir",
		Email:          "jahangir@mail.com",
		EmailConfirmed: false,
		NumFollowing:   2,
		NumFollowers:   20,
		AppId:          myCokeApp.Id,
		Msg:            []byte("msg9876"),
	}

	// Make sure the 2 users  are sorted in ascending order by ID
	if user1.Id >= user2.Id {
		user1, user2 = user2, user1
	}

	return &myCokeApp, &user1, &user2
}
