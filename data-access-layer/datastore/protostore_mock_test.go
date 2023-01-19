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

	"github.com/stretchr/testify/mock"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/test/pb"
)

func TestProtoStoreMock(t *testing.T) {
	var rowsAffected int64 = 0

	var myMockProtoStore *ProtoStoreMock = &ProtoStoreMock{}
	myMockProtoStore.On("DropTables", mock.Anything).Return(nil)
	myMockProtoStore.On("Register", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	myMockProtoStore.On("Insert", mock.Anything, mock.Anything, mock.Anything).Return(rowsAffected, Metadata{}, nil)
	myMockProtoStore.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(rowsAffected, Metadata{}, nil)
	myMockProtoStore.On("Upsert", mock.Anything, mock.Anything, mock.Anything).Return(rowsAffected, Metadata{}, nil)
	myMockProtoStore.On("FindById", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	myMockProtoStore.On("FindAll", mock.Anything, mock.Anything).Return(map[string]Metadata{}, nil)
	myMockProtoStore.On("FindAllAsMap", mock.Anything, mock.Anything).Return(map[string]Metadata{}, nil)
	myMockProtoStore.On("DeleteById", mock.Anything, mock.Anything, mock.Anything).Return(rowsAffected, nil)
	myMockProtoStore.On("InsertWithMetadata", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(rowsAffected, Metadata{}, nil)
	myMockProtoStore.On("UpdateWithMetadata", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(rowsAffected, Metadata{}, nil)
	myMockProtoStore.On("UpsertWithMetadata", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(rowsAffected, Metadata{}, nil)
	myMockProtoStore.On("GetMetadata", mock.Anything, mock.Anything, mock.Anything).Return(Metadata{}, nil)
	myMockProtoStore.On("GetRevision", mock.Anything, mock.Anything, mock.Anything).Return(rowsAffected, nil)
	myMockProtoStore.On("Configure", mock.Anything, mock.Anything, mock.Anything).Return()
	myMockProtoStore.On("GetAuthorizer").Return(MetadataBasedAuthorizer{})

	// Confirm that ProtoStoreMock implements all methods of ProtoStore
	// Modifying the signature of any method of ProtoStore will lead to the line below not compiling
	var myProtoStore ProtoStore = myMockProtoStore

	ctx := context.TODO()
	myProtoStore.Configure(ctx, false, MetadataBasedAuthorizer{})
	_ = myProtoStore.DropTables(&pb.CPU{}, &pb.Memory{})
	_ = myProtoStore.Register(ctx, nil, &pb.CPU{}, &pb.Memory{})
	_, _, _ = myProtoStore.Insert(ctx, "001", &pb.CPU{})
	_, _, _ = myProtoStore.Update(ctx, "001", &pb.CPU{})
	_, _, _ = myProtoStore.Upsert(ctx, "001", &pb.CPU{})
	_ = myProtoStore.FindById(ctx, "001", &pb.CPU{}, nil)
	_, _ = myProtoStore.FindAll(ctx, nil)
	_, _ = myProtoStore.FindAllAsMap(ctx, nil)
	_, _ = myProtoStore.DeleteById(ctx, "001", &pb.CPU{})
	_, _, _ = myProtoStore.InsertWithMetadata(ctx, "001", &pb.CPU{}, Metadata{})
	_, _, _ = myProtoStore.UpdateWithMetadata(ctx, "001", &pb.CPU{}, Metadata{})
	_, _, _ = myProtoStore.UpsertWithMetadata(ctx, "001", &pb.CPU{}, Metadata{})
	_, _ = myProtoStore.GetMetadata(ctx, "001", &pb.CPU{})
	_, _ = myProtoStore.GetRevision(ctx, "001", &pb.CPU{})
	_ = myProtoStore.GetAuthorizer()

	// Check that each ProtoStore method invocation is actually registered in the mock
	myMockProtoStore.AssertCalled(t, "DropTables", mock.Anything)
	myMockProtoStore.AssertCalled(t, "Register", mock.Anything, mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "Insert", mock.Anything, mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "Update", mock.Anything, mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "FindById", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "FindAll", mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "FindAllAsMap", mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "DeleteById", mock.Anything, mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "InsertWithMetadata", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "UpdateWithMetadata", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "GetMetadata", mock.Anything, mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "GetRevision", mock.Anything, mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "Configure", mock.Anything, mock.Anything, mock.Anything)
	myMockProtoStore.AssertCalled(t, "GetAuthorizer")
}
