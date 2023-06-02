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

// This package exposes interface [pkg.protostore.ProtoStore] to the consumer, which is a wrapper
// around [pkg.datastore.DataStore] interface and is used specifically to persist Protobuf messages.
// Just as with [pkg.datastore.DataStore], Protobuf messages can be persisted with revisioning and
// multi-tenancy support along with `CreatedAt` and `UpdatedAt` timestamps.
// Tombstone Delete or soft deletes are supported with `DeletedAt` struct field.
// Use Delete to remove any records that are soft deleted but still in database
package protostore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
)

type ProtoStore interface {
	Register(ctx context.Context, roleMapping map[string]dbrole.DbRole, msgs ...proto.Message) error
	Insert(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error)
	Update(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error)
	Upsert(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error)
	FindById(ctx context.Context, id string, msg proto.Message, metadata *Metadata) error
	FindAll(ctx context.Context, msgs interface{}, pagination *datastore.Pagination) (metadataMap map[string]Metadata, err error)
	FindAllAsMap(ctx context.Context, msgsMap interface{}, pagination *datastore.Pagination) (metadataMap map[string]Metadata, err error)
	SoftDeleteById(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, err error)
	DeleteById(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, err error)

	InsertWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (rowsAffected int64, md Metadata, err error)
	UpdateWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (rowsAffected int64, md Metadata, err error)
	UpsertWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (rowsAffected int64, md Metadata, err error)

	GetMetadata(ctx context.Context, id string, msg proto.Message) (md Metadata, err error)
	GetRevision(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, err error)

	MsgToFilter(ctx context.Context, id string, msg proto.Message) (pMsg *ProtoStoreMsg, err error)
	MsgToPersist(ctx context.Context, id string, msg proto.Message, md Metadata) (pMsg *ProtoStoreMsg, err error)

	GetAuthorizer() authorizer.Authorizer
	DropTables(msgs ...proto.Message) error
}

type Metadata struct {
	Id        string
	ParentId  string
	Revision  int64
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt
}

type ProtoStoreMsg struct {
	Id         string `gorm:"primaryKey"`
	Msg        []byte
	ParentId   string
	Revision   int64
	OrgId      string `gorm:"primaryKey"`
	InstanceId string `gorm:"primaryKey"`
	CreatedAt  time.Time
	UpdatedAt  time.Time
	DeletedAt  gorm.DeletedAt
	XTableName string `gorm:"-"`
}

func (msg *ProtoStoreMsg) TableName() string {
	return msg.XTableName
}

func FromBytes(bytes []byte, message proto.Message) error {
	err := proto.Unmarshal(bytes, message)
	if err != nil {
		return ErrUnmarshalling.Wrap(err)
	}
	return nil
}

func ToBytes(message proto.Message) ([]byte, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, ErrMarshalling.Wrap(err)
	}
	return data, nil
}

func MetadataFrom(protoStoreMsg ProtoStoreMsg) Metadata {
	return Metadata{
		Id:        protoStoreMsg.Id,
		ParentId:  protoStoreMsg.ParentId,
		Revision:  protoStoreMsg.Revision,
		CreatedAt: protoStoreMsg.CreatedAt,
		UpdatedAt: protoStoreMsg.UpdatedAt,
		DeletedAt: protoStoreMsg.DeletedAt,
	}
}

type ProtobufDataStore struct {
	ds     datastore.DataStore
	logger *logrus.Entry
}

func GetProtoStore(logger *logrus.Entry, ds datastore.DataStore) ProtoStore {
	p := ProtobufDataStore{
		ds:     ds,
		logger: logger,
	}
	return p
}

func (p ProtobufDataStore) GetAuthorizer() authorizer.Authorizer {
	return p.ds.GetAuthorizer()
}

func (p ProtobufDataStore) Register(ctx context.Context, roleMapping map[string]dbrole.DbRole, msgs ...proto.Message) error {
	for _, msg := range msgs {
		pMsg := &ProtoStoreMsg{XTableName: datastore.GetTableName(msg)}
		err := p.ds.Helper().RegisterHelper(ctx, roleMapping, datastore.GetTableName(msg), pMsg)
		if err != nil {
			p.logger.Errorf("Registering proto message %s failed with error: %v", msg, err)
			return err
		}
	}
	return nil
}

// @DEPRECATED See [InsertWithMetadata].
func (p ProtobufDataStore) Insert(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error) {
	return p.InsertWithMetadata(ctx, id, msg, Metadata{})
}

// Inserts a new Protobuf record in the DB.
// Returns,
// rowsAffected - 0 if insertion fails; 1 otherwise
// md - metadata of the new Protobuf record
// err - error that occurred during insertion, if any.
func (p ProtobufDataStore) InsertWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (rowsAffected int64, md Metadata, err error) {
	protoStoreMsg, err := p.MsgToPersist(ctx, id, msg, metadata)
	if err != nil {
		return 0, Metadata{}, err
	}
	// We cannot use 0 as the starting revision as we cannot
	// differentiate between 0 vs unset values.
	// Hence, we are using 1 as the first revision for the
	// inserted records unless they are already at revision != 0.
	if protoStoreMsg.Revision == 0 {
		protoStoreMsg.Revision = 1
	}

	rowsAffected, err = p.ds.Insert(ctx, protoStoreMsg)
	if err != nil {
		return 0, Metadata{}, err
	}
	metadata = MetadataFrom(*protoStoreMsg)
	return rowsAffected, metadata, nil
}

// Update Fetches metadata for the record and updates the Protobuf message.
// NOTE: Avoid using this method in user-workflows and only in service-to-service workflows
// when the updates are already ordered by some other service/app.
func (p ProtobufDataStore) Update(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error) {
	md, err = p.GetMetadata(ctx, id, msg)
	if err != nil {
		return 0, Metadata{}, err
	}
	return p.UpdateWithMetadata(ctx, id, msg, md)
}

// Updates an existing Protobuf record in the DB.
// Returns,
// rowsAffected - 0 if update fails; 1 otherwise
// md - metadata of the updated Protobuf record
// err - error that occurred during update, if any.
func (p ProtobufDataStore) UpdateWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (rowsAffected int64, md Metadata, err error) {
	protoStoreMsg, err := p.MsgToPersist(ctx, id, msg, metadata)
	if err != nil {
		return 0, Metadata{}, err
	}

	rowsAffected, err = p.ds.Update(ctx, protoStoreMsg)
	metadata = MetadataFrom(*protoStoreMsg)
	if err != nil {
		return 0, Metadata{}, err
	} else if rowsAffected == 1 {
		metadata.Revision++ // Update increments the revision
	}
	return rowsAffected, metadata, nil
}

// Upsert Fetches metadata for the record and upserts the Protobuf message.
// NOTE: Avoid using this method in user-workflows and only in service-to-service workflows
// when the updates are already ordered by some other service/app.
func (p ProtobufDataStore) Upsert(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error) {
	md, err = p.GetMetadata(ctx, id, msg)
	if err != nil && !errors.Is(err, ErrRecordNotFound) {
		return 0, Metadata{}, err
	}

	// If Protobuf record was not found, then Upsert will be an insertion and initial revision of 1 will be used
	if errors.Is(err, ErrRecordNotFound) {
		md.Revision = 1
	}

	return p.UpsertWithMetadata(ctx, id, msg, md)
}

// Upserts a Protobuf record in the DB (if the record exists, it is updated; if it does not, it is inserted).
// Returns,
// rowsAffected - 0 if upsert fails; 1 otherwise
// md - metadata of the upserted Protobuf record
// err - error that occurred during upsert, if any.
func (p ProtobufDataStore) UpsertWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (
	rowsAffected int64, md Metadata, err error,
) {
	protoStoreMsg, err := p.MsgToPersist(ctx, id, msg, metadata)
	if err != nil {
		return 0, Metadata{}, err
	}

	rowsAffected, err = p.ds.Upsert(ctx, protoStoreMsg)
	if err != nil {
		return 0, Metadata{}, err
	}

	md, err = p.GetMetadata(ctx, id, msg)
	if err != nil {
		return 0, Metadata{}, err
	}
	return rowsAffected, md, nil
}

// Finds a Protobuf message by ID.
// If metadata arg. is non-nil, fills it with the metadata (parent ID & revision) of the Protobuf message that was found.
func (p ProtobufDataStore) FindById(ctx context.Context, id string, msg proto.Message, metadata *Metadata) error {
	protoStoreMsg, err := p.MsgToFilter(ctx, id, msg)
	if err != nil {
		return err
	}

	err = p.ds.Find(ctx, protoStoreMsg)
	if err != nil {
		return err
	}
	if metadata != nil {
		*metadata = MetadataFrom(*protoStoreMsg)
	}
	return FromBytes(protoStoreMsg.Msg, msg)
}

func (p ProtobufDataStore) GetMetadata(ctx context.Context, id string, msg proto.Message) (md Metadata, err error) {
	protoStoreMsg, err := p.MsgToFilter(ctx, id, msg)
	if err != nil {
		return md, err
	}
	err = p.ds.Find(ctx, protoStoreMsg)
	return MetadataFrom(*protoStoreMsg), err
}

func (p ProtobufDataStore) GetRevision(ctx context.Context, id string, msg proto.Message) (int64, error) {
	// TODO: Optimize to read revision only
	md, err := p.GetMetadata(ctx, id, msg)
	if err != nil {
		return -1, err
	}
	return md.Revision, err
}

func (p ProtobufDataStore) FindAllAsMap(ctx context.Context, msgsMap interface{}, pagination *datastore.Pagination) (metadataMap map[string]Metadata, err error) {
	// Type of value in msgsMap
	var elemType reflect.Type
	switch reflect.TypeOf(msgsMap).Kind() {
	case reflect.Map:
		elemType = reflect.TypeOf(msgsMap).Elem()
	default:
		errMsg := "\"msgsMap\" argument has to be a map"
		p.logger.Error(errMsg)
		err = ErrNotPtrToStructSlice.Wrap(fmt.Errorf(errMsg))
		return nil, err
	}

	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		errMsg := "\"msgsMap\" argument has to be a map with Protobuf struct or a pointer to one as a value"
		p.logger.Error(errMsg)
		err = ErrNotPtrToStructSlice.Wrap(fmt.Errorf(errMsg))
		return nil, err
	}

	tableName := datastore.GetTableName(reflect.New(elemType).Interface())

	protoStoreMsgs := make([]ProtoStoreMsg, 0)
	err = p.ds.Helper().FindAllInTable(ctx, tableName, &protoStoreMsgs, pagination)
	if err != nil {
		return nil, err
	}

	metadataMap = make(map[string]Metadata, len(protoStoreMsgs))
	msgsMapValue := reflect.ValueOf(msgsMap)
	isMsgsElemPtrToStructs := reflect.TypeOf(msgsMap).Elem().Kind() == reflect.Ptr

	for _, protoStoreMsg := range protoStoreMsgs {
		// Empty instance of a Protobuf message
		msgCopy := reflect.New(elemType).Interface().(proto.Message)
		if err = FromBytes(protoStoreMsg.Msg, msgCopy); err != nil {
			return nil, err
		}

		if isMsgsElemPtrToStructs { // Insert a pointer to a Protobuf message into output map
			msgsMapValue.SetMapIndex(reflect.ValueOf(protoStoreMsg.Id), reflect.ValueOf(msgCopy))
		} else if !isMsgsElemPtrToStructs { // Insert a Protobuf message into output map
			msgsMapValue.SetMapIndex(reflect.ValueOf(protoStoreMsg.Id), reflect.ValueOf(msgCopy).Elem())
		}
		metadataMap[protoStoreMsg.Id] = MetadataFrom(protoStoreMsg)
	}

	return metadataMap, nil
}

// FindAll Finds all messages (of the same type as the element of msgs) in Protostore and stores the result in msgs.
// msgs must be a pointer to a slice of Protobuf structs or a pointer to a slice of pointers to Protobuf structs.
// It will be modified in-place.
// Returns a map of Protobuf messages' IDs to their metadata (parent ID & revision).
func (p ProtobufDataStore) FindAll(ctx context.Context, msgs interface{}, pagination *datastore.Pagination) (metadataMap map[string]Metadata, err error) {
	if reflect.TypeOf(msgs).Kind() != reflect.Ptr || reflect.TypeOf(msgs).Elem().Kind() != reflect.Slice {
		errMsg := "\"msgs\" argument has to be a pointer to a slice"
		p.logger.Error(errMsg)
		err = ErrNotPtrToStructSlice.Wrap(fmt.Errorf(errMsg))
		return nil, err
	}

	// Type of element that msgs slice consists of (either a protobuf message or a pointer to protobuf message)
	sliceElemType := reflect.TypeOf(msgs).Elem().Elem()

	// True if msgs is a pointer to a slice of pointers to structs
	// False if msgs is a pointer to a slice of structs
	isSlicePtrToStructs := reflect.TypeOf(msgs).Elem().Elem().Kind() == reflect.Ptr
	tableName := datastore.GetTableName(msgs)

	protoStoreMsgs := make([]ProtoStoreMsg, 0)
	err = p.ds.Helper().FindAllInTable(ctx, tableName, &protoStoreMsgs, pagination)
	if err != nil {
		return nil, err
	}

	output := reflect.MakeSlice(reflect.SliceOf(sliceElemType), 0, len(protoStoreMsgs))
	metadataMap = make(map[string]Metadata, len(protoStoreMsgs))

	for _, protoStoreMsg := range protoStoreMsgs {
		var msgCopy proto.Message // Empty instance of a Protobuf message
		if isSlicePtrToStructs {
			msgCopy = reflect.New(sliceElemType.Elem()).Interface().(proto.Message)
		} else {
			msgCopy = reflect.New(sliceElemType).Interface().(proto.Message)
		}

		if err = FromBytes(protoStoreMsg.Msg, msgCopy); err != nil {
			return nil, err
		}

		if isSlicePtrToStructs { // Append a pointer to Protobuf message to output slice
			output = reflect.Append(output, reflect.ValueOf(msgCopy))
		} else { // Append a Protobuf message to output slice
			output = reflect.Append(output, reflect.ValueOf(msgCopy).Elem())
		}
		metadataMap[protoStoreMsg.Id] = MetadataFrom(protoStoreMsg)
	}

	reflect.ValueOf(msgs).Elem().Set(output)
	return metadataMap, nil
}

func (p ProtobufDataStore) SoftDeleteById(ctx context.Context, id string, msg proto.Message) (int64, error) {
	protoStoreMsg, err := p.MsgToFilter(ctx, id, msg)
	if err != nil {
		return 0, err
	}
	return p.ds.SoftDelete(ctx, protoStoreMsg)
}

func (p ProtobufDataStore) DeleteById(ctx context.Context, id string, msg proto.Message) (int64, error) {
	protoStoreMsg, err := p.MsgToFilter(ctx, id, msg)
	if err != nil {
		return 0, err
	}
	return p.ds.Delete(ctx, protoStoreMsg)
}

func (p ProtobufDataStore) DropTables(msgs ...proto.Message) error {
	for _, msg := range msgs {
		p.logger.Infof("Dropping Table for %s", msg)
		err := p.ds.TestHelper().DropTables(datastore.GetTableName(msg))
		if err != nil {
			return err
		}
	}
	return nil
}

// Return the serialized ProtoStoreMsg that can be persisted to database
// and error that occurred during extraction orgId from context, or serialization.
func (p ProtobufDataStore) MsgToPersist(ctx context.Context, id string, msg proto.Message, md Metadata) (pMsg *ProtoStoreMsg, err error) {
	pMsg, err = p.MsgToFilter(ctx, id, msg)
	if err != nil {
		return &ProtoStoreMsg{}, err
	}
	bytes, err := ToBytes(msg)
	if err != nil {
		return &ProtoStoreMsg{}, err
	}

	// Fill up the actual protobuf message and metadata from md
	pMsg.Msg = bytes
	pMsg.ParentId = md.ParentId
	pMsg.Revision = md.Revision
	return pMsg, nil
}

// Return the ProtoStoreMsg that can be used for filtering with id/orgId filled up
// and error that occurred during extraction orgId from context.
func (p ProtobufDataStore) MsgToFilter(ctx context.Context, id string, msg proto.Message) (pMsg *ProtoStoreMsg, err error) {
	orgId, err := p.ds.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return &ProtoStoreMsg{}, err
	}

	instanceId := ""
	if p.ds.GetInstancer() != nil {
		instanceId, err = p.ds.GetInstancer().GetInstanceId(ctx)
		if err != nil {
			return &ProtoStoreMsg{}, err
		}
	}

	return &ProtoStoreMsg{
		Id:         id,
		OrgId:      orgId,
		InstanceId: instanceId,
		XTableName: datastore.GetTableName(msg),
	}, nil
}
