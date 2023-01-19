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

	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
)

/*
ProtoStore mock. Can be imported by micro-services that need to mock ProtoStore.
*/
type ProtoStoreMock struct {
	mock.Mock
}

func (p *ProtoStoreMock) Update(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error) {
	args := p.Called(ctx, id, msg)
	return args.Get(0).(int64), args.Get(1).(Metadata), args.Error(2)
}

func (p *ProtoStoreMock) Upsert(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error) {
	args := p.Called(ctx, id, msg)
	return args.Get(0).(int64), args.Get(1).(Metadata), args.Error(2)
}

func (p *ProtoStoreMock) FindAllAsMap(ctx context.Context, msgsMap interface{}) (metadataMap map[string]Metadata, err error) {
	args := p.Called(ctx, msgsMap)
	return args.Get(0).(map[string]Metadata), args.Error(1)
}

func (p *ProtoStoreMock) GetMetadata(ctx context.Context, id string, msg proto.Message) (md Metadata, err error) {
	args := p.Called(ctx, id, msg)
	return args.Get(0).(Metadata), args.Error(1)
}

func (p *ProtoStoreMock) GetRevision(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, err error) {
	args := p.Called(ctx, id, msg)
	return args.Get(0).(int64), args.Error(1)
}

func (p *ProtoStoreMock) DropTables(msgs ...proto.Message) error {
	args := p.Called(msgs)
	return args.Error(0)
}

func (p *ProtoStoreMock) Register(ctx context.Context, roleMapping map[string]DbRole, msgs ...proto.Message) error {
	args := p.Called(ctx, roleMapping, msgs)
	return args.Error(0)
}

func (p *ProtoStoreMock) Insert(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error) {
	args := p.Called(ctx, id, msg)
	return args.Get(0).(int64), args.Get(1).(Metadata), args.Error(2)
}

func (p *ProtoStoreMock) FindById(ctx context.Context, id string, msg proto.Message, metadata *Metadata) error {
	args := p.Called(ctx, id, msg, metadata)
	return args.Error(0)
}

func (p *ProtoStoreMock) FindAll(ctx context.Context, msgs interface{}) (map[string]Metadata, error) {
	args := p.Called(ctx, msgs)
	return args.Get(0).(map[string]Metadata), args.Error(1)
}

func (p *ProtoStoreMock) DeleteById(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, err error) {
	args := p.Called(ctx, id, msg)
	return args.Get(0).(int64), args.Error(1)
}

func (p *ProtoStoreMock) InsertWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (
	rowsAffected int64, md Metadata, err error,
) {
	args := p.Called(ctx, id, msg, metadata)
	return args.Get(0).(int64), args.Get(1).(Metadata), args.Error(2)
}

func (p *ProtoStoreMock) UpdateWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (
	rowsAffected int64, md Metadata, err error,
) {
	args := p.Called(ctx, id, msg, metadata)
	return args.Get(0).(int64), args.Get(1).(Metadata), args.Error(2)
}

func (p *ProtoStoreMock) UpsertWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (
	rowsAffected int64, md Metadata, err error,
) {
	args := p.Called(ctx, id, msg, metadata)
	return args.Get(0).(int64), args.Get(1).(Metadata), args.Error(2)
}

func (p *ProtoStoreMock) Configure(ctx context.Context, isDataStoreInMemory bool, authorizer Authorizer) {
	p.Called(ctx, isDataStoreInMemory, authorizer)
}

func (p *ProtoStoreMock) GetAuthorizer() Authorizer {
	args := p.Called()
	return args.Get(0).(Authorizer)
}
