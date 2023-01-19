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
	"errors"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

type ProtoStore interface {
	Register(ctx context.Context, roleMapping map[string]DbRole, msgs ...proto.Message) error
	Insert(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error)
	Update(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error)
	Upsert(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error)
	FindById(ctx context.Context, id string, msg proto.Message, metadata *Metadata) error
	FindAll(ctx context.Context, msgs interface{}) (metadataMap map[string]Metadata, err error)
	FindAllAsMap(ctx context.Context, msgsMap interface{}) (metadataMap map[string]Metadata, err error)
	DeleteById(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, err error)

	/*
		Inserts a new Protobuf record in the DB.
		Input parameters:
		ctx - Golang context to use
		id - unique ID of the Protobuf record
		msg - Protobuf record
		metadata - metadata to use when inserting Protobuf record

		Output parameters:
		rowsAffected - 0 if insertion fails; 1 otherwise
		md - metadata of the new Protobuf record
		err - error that occurred during insertion, if any
	*/
	InsertWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (rowsAffected int64, md Metadata, err error)

	/*
		Updates an existing Protobuf record in the DB.
		Input parameters:
		ctx - Golang context to use
		id - unique ID of the Protobuf record
		msg - Protobuf record
		metadata - metadata to use when updating Protobuf record

		Output parameters:
		rowsAffected - 0 if update fails; 1 otherwise
		md - metadata of the updated Protobuf record
		err - error that occurred during update, if any

	*/
	UpdateWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (rowsAffected int64, md Metadata, err error)

	/*
		Upserts a Protobuf record in the DB (if the record exists, it is updated; if it does not, it is inserted).
		Input parameters:
		ctx - Golang context to use
		id - unique ID of the Protobuf record
		msg - Protobuf record
		metadata - metadata to use when upserting Protobuf record

		Output parameters:
		rowsAffected - 0 if upsert fails; 1 otherwise
		md - metadata of the upserted Protobuf record
		err - error that occurred during upsert, if any

	*/
	UpsertWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (rowsAffected int64, md Metadata, err error)

	GetMetadata(ctx context.Context, id string, msg proto.Message) (md Metadata, err error)
	GetRevision(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, err error)

	Configure(ctx context.Context, isDataStoreInMemory bool, authorizer Authorizer)
	GetAuthorizer() Authorizer
	DropTables(msgs ...proto.Message) error
}

type Metadata struct {
	Id       string
	ParentId string
	Revision int64
}
type ProtoStoreMsg struct {
	Id       string `db_column:"id" primary_key:"true"`
	Msg      []byte `db_column:"msg"`
	ParentId string `db_column:"_parent_id"`
	Revision int64  `db_column:"_revision"`
	OrgId    string `db_column:"org_id"`
}

func (protoStoreMsg ProtoStoreMsg) GetId() []interface{} {
	return []interface{}{protoStoreMsg.Id, protoStoreMsg.OrgId}
}

func FromBytes(bytes []byte, message proto.Message) error {
	err := proto.Unmarshal(bytes, message)
	if err != nil {
		return ErrorUnmarshalling.Wrap(err)
	}
	return nil
}

func ToBytes(message proto.Message) ([]byte, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, ErrorMarshalling.Wrap(err)
	}
	return data, nil
}

func MetadataFrom(protoStoreMsg ProtoStoreMsg) Metadata {
	return Metadata{
		Id:       protoStoreMsg.Id,
		ParentId: protoStoreMsg.ParentId,
		Revision: protoStoreMsg.Revision,
	}
}

type ProtobufDataStore struct{}

func (p ProtobufDataStore) GetAuthorizer() Authorizer {
	return Helper.GetAuthorizer()
}

func GetProtoStore() ProtoStore {
	return ProtobufDataStore{}
}

func (p ProtobufDataStore) Configure(ctx context.Context, isDataStoreInMemory bool, authorizer Authorizer) {
	Helper.Configure(ctx, isDataStoreInMemory, authorizer)
}

func (p ProtobufDataStore) Register(ctx context.Context, roleMapping map[string]DbRole, msgs ...proto.Message) error {
	for _, msg := range msgs {
		err := Helper.RegisterWithDALHelper(ctx, roleMapping, GetTableName(msg), ProtoStoreMsg{})
		if err != nil {
			logger.Errorf("Registering proto message %s failed with error: %v", msg, err)
			return err
		}
	}
	return nil
}

/*
Inserts a Protobuf message with the given id and belonging to the given org. into data store.
*/
func (p ProtobufDataStore) Insert(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error) {
	return p.InsertWithMetadata(ctx, id, msg, Metadata{})
}

func (p ProtobufDataStore) InsertWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (rowsAffected int64, md Metadata, err error) {
	bytes, err := ToBytes(msg)
	if err != nil {
		return 0, Metadata{}, err
	}

	orgId, err := Helper.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return 0, Metadata{}, err
	}

	protoStoreMsg := ProtoStoreMsg{
		Id:       id,
		Msg:      bytes,
		ParentId: metadata.ParentId,
		Revision: metadata.Revision,
		OrgId:    orgId,
	}
	// We cannot use 0 as the starting revision as we cannot
	// differentiate between 0 vs unset values.
	// Hence we are using 1 as the first revision for the
	// inserted records unless they a revision != 0.
	if protoStoreMsg.Revision == 0 {
		protoStoreMsg.Revision = 1
	}

	rowsAffected, err = Helper.InsertInTable(ctx, GetTableName(msg), protoStoreMsg)
	if err != nil {
		return 0, Metadata{}, err
	}
	metadata = MetadataFrom(protoStoreMsg)
	return rowsAffected, metadata, nil
}

/*
Fetches metadata for the record and updates the Protobuf message.
NOTE: Avoid using this method in user-workflows and only in service-to-service workflows
when the updates are already ordered by some other service/app.
*/
func (p ProtobufDataStore) Update(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error) {
	md, err = p.GetMetadata(ctx, id, msg)
	if err != nil {
		return 0, Metadata{}, err
	}
	return p.UpdateWithMetadata(ctx, id, msg, md)
}

/*
Updates a Protobuf message.
*/
func (p ProtobufDataStore) UpdateWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (rowsAffected int64, md Metadata, err error) {
	bytes, err := ToBytes(msg)
	if err != nil {
		return 0, Metadata{}, err
	}

	orgId, err := Helper.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return 0, Metadata{}, err
	}

	protoStoreMsg := ProtoStoreMsg{
		Id:       id,
		Msg:      bytes,
		ParentId: metadata.ParentId,
		Revision: metadata.Revision,
		OrgId:    orgId,
	}

	rowsAffected, err = Helper.UpdateInTable(ctx, GetTableName(msg), protoStoreMsg)
	metadata = MetadataFrom(protoStoreMsg)
	if err != nil {
		return 0, Metadata{}, err
	} else if rowsAffected == 1 {
		metadata.Revision++ // Update increments the revision
	}
	return rowsAffected, metadata, nil
}

/*
Fetches metadata for the record and upserts the Protobuf message.
NOTE: Avoid using this method in user-workflows and only in service-to-service workflows
when the updates are already ordered by some other service/app.
*/
func (p ProtobufDataStore) Upsert(ctx context.Context, id string, msg proto.Message) (rowsAffected int64, md Metadata, err error) {
	md, err = p.GetMetadata(ctx, id, msg)
	if err != nil && !errors.Is(err, RecordNotFoundError) {
		return 0, Metadata{}, err
	}

	// If Protobuf record was not found, then Upsert will be an insertion and initial revision of 1 will be used
	if errors.Is(err, RecordNotFoundError) {
		md.Revision = 1
	}
	return p.UpsertWithMetadata(ctx, id, msg, md)
}

func (p ProtobufDataStore) UpsertWithMetadata(ctx context.Context, id string, msg proto.Message, metadata Metadata) (
	rowsAffected int64, md Metadata, err error,
) {
	bytes, err := ToBytes(msg)
	if err != nil {
		return 0, Metadata{}, err
	}

	orgId, err := Helper.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return 0, Metadata{}, err
	}

	protoStoreMsg := ProtoStoreMsg{
		Id:       id,
		Msg:      bytes,
		ParentId: metadata.ParentId,
		Revision: metadata.Revision,
		OrgId:    orgId,
	}

	rowsAffected, err = Helper.UpsertInTable(ctx, GetTableName(msg), protoStoreMsg)
	if err != nil {
		return 0, Metadata{}, err
	}

	md, err = p.GetMetadata(ctx, id, msg)
	if err != nil {
		return 0, Metadata{}, err
	}

	return rowsAffected, md, nil
}

/*
Finds a Protobuf message by ID.
If metadata arg. is non-nil, fills it with the metadata (parent ID & revision) of the Protobuf message that was found.
*/
func (p ProtobufDataStore) FindById(ctx context.Context, id string, msg proto.Message, metadata *Metadata) error {
	orgId, err := Helper.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return err
	}

	protoStoreMsg := ProtoStoreMsg{
		Id:    id,
		OrgId: orgId,
	}
	err = Helper.FindInTable(ctx, GetTableName(msg), &protoStoreMsg)
	if err != nil {
		return err
	}
	if metadata != nil {
		*metadata = MetadataFrom(protoStoreMsg)
	}
	return FromBytes(protoStoreMsg.Msg, msg)
}

func (p ProtobufDataStore) GetMetadata(ctx context.Context, id string, msg proto.Message) (md Metadata, err error) {
	orgId, err := Helper.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return md, err
	}

	protoStoreMsg := ProtoStoreMsg{
		Id:    id,
		OrgId: orgId,
	}
	err = Helper.FindInTable(ctx, GetTableName(msg), &protoStoreMsg)
	return MetadataFrom(protoStoreMsg), err
}

func (p ProtobufDataStore) GetRevision(ctx context.Context, id string, msg proto.Message) (int64, error) {
	// TODO: Optimize to read revision only
	md, err := p.GetMetadata(ctx, id, msg)
	if err != nil {
		return -1, err
	}
	return md.Revision, err
}

func (p ProtobufDataStore) FindAllAsMap(ctx context.Context, msgsMap interface{}) (metadataMap map[string]Metadata, err error) {
	// Type of value in msgsMap
	var elemType reflect.Type
	switch reflect.TypeOf(msgsMap).Kind() {
	case reflect.Map:
		elemType = reflect.TypeOf(msgsMap).Elem()
	default:
		errMsg := "\"msgsMap\" argument has to be a map"
		logger.Error(errMsg)
		err = IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return nil, err
	}

	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		errMsg := "\"msgsMap\" argument has to be a map with Protobuf struct or a pointer to one as a value"
		logger.Error(errMsg)
		err = IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return nil, err
	}

	/*
		True if msgsMap is a map of strings to POINTERS OF STRUCTS
		False if msgsMap is a map of strings to STRUCTS
	*/
	var isMsgsElemPtrToStructs bool = reflect.TypeOf(msgsMap).Elem().Kind() == reflect.Ptr
	tableName := GetTableNameFromSlice(msgsMap)

	protoStoreMsgs := make([]ProtoStoreMsg, 0)
	err = Helper.FindAllInTable(ctx, tableName, &protoStoreMsgs)
	if err != nil {
		return nil, err
	}

	metadataMap = make(map[string]Metadata, len(protoStoreMsgs))
	msgsMapValue := reflect.ValueOf(msgsMap)

	for _, protoStoreMsg := range protoStoreMsgs {
		// Empty instance of a Protobuf message
		var msgCopy proto.Message = reflect.New(elemType).Interface().(proto.Message)
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

/*
Finds all messages (of the same type as the element of msgs) in Protostore and stores the result in msgs.
msgs must be a pointer to a slice of Protobuf structs or a pointer to a slice of pointers to Protobuf structs.
It will be modified in-place.
Returns a map of Protobuf messages' IDs to their metadata (parent ID & revision).
*/
func (p ProtobufDataStore) FindAll(ctx context.Context, msgs interface{}) (metadataMap map[string]Metadata, err error) {
	if reflect.TypeOf(msgs).Kind() != reflect.Ptr || reflect.TypeOf(msgs).Elem().Kind() != reflect.Slice {
		errMsg := "\"msgs\" argument has to be a pointer to a slice"
		logger.Error(errMsg)
		err = IllegalArgumentError.Wrap(fmt.Errorf(errMsg))
		return nil, err
	}

	// Type of element that msgs slice consists of (either a protobuf message or a pointer to protobuf message)
	sliceElemType := reflect.TypeOf(msgs).Elem().Elem()

	/*
		True if msgs is a pointer to a slice of pointers to structs
		False if msgs is a pointer to a slice of structs
	*/
	var isSlicePtrToStructs bool = reflect.TypeOf(msgs).Elem().Elem().Kind() == reflect.Ptr
	tableName := GetTableNameFromSlice(msgs)

	protoStoreMsgs := make([]ProtoStoreMsg, 0)
	err = Helper.FindAllInTable(ctx, tableName, &protoStoreMsgs)
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

func (p ProtobufDataStore) DeleteById(ctx context.Context, id string, msg proto.Message) (int64, error) {
	orgId, err := Helper.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return 0, err
	}

	protoStoreMsg := ProtoStoreMsg{
		Id:    id,
		OrgId: orgId,
	}
	return Helper.DeleteInTable(ctx, GetTableName(msg), protoStoreMsg)
}

func (p ProtobufDataStore) DropTables(msgs ...proto.Message) error {
	for _, msg := range msgs {
		logger.Infof("Dropping Table for %s", msg)
		err := TestHelper.Drop(GetTableName(msg))
		if err != nil {
			return err
		}
	}
	return nil
}
