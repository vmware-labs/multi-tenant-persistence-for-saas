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

	"google.golang.org/protobuf/proto"
)

type Status int

const (
	PENDING Status = iota //Indicates that enforcement needs to be established after resource is created/modified
	REALIZED
	ERROR
	DELETION_PENDING
	DELETION_REALIZED //TODO consider deleting this status value
)

type ProtobufWithMetadata struct {
	proto.Message
	Metadata
}

type IRealizationStore interface {
	FindById(ctx context.Context, id string, resource *ProtobufWithMetadata) error

	Upsert(ctx context.Context, resource ProtobufWithMetadata) error
	MarkEnforcementAsPending(ctx context.Context, enforcementPoint string, resources ...ProtobufWithMetadata) error
	MarkEnforcementAsSuccess(ctx context.Context, enforcementPoint string, resources ...ProtobufWithMetadata) error
	MarkEnforcementAsError(ctx context.Context, enforcementPoint string, error string, resources ...ProtobufWithMetadata) error

	Delete(ctx context.Context, resource ProtobufWithMetadata) error
	MarkEnforcementAsDeletionPending(ctx context.Context, enforcementPoint string, resources ...ProtobufWithMetadata) error
	MarkEnforcementAsDeletionRealized(ctx context.Context, enforcementPoint string, resources ...ProtobufWithMetadata) error

	GetOverallStatus(ctx context.Context, resource ProtobufWithMetadata) (Status, error)
	GetOverallStatusWithEnforcementDetails(ctx context.Context, resource ProtobufWithMetadata) (Status, map[string]Status, error)
}

type RealizationStore struct {
	//Helper function provided by the user that determines enforcement points for a resource
	GetEnforcementPoints func(ctx context.Context, resource ProtobufWithMetadata) (enforcementPoints []string)
}

/*
Finds a resource by ID.
*/
func (r *RealizationStore) FindById(ctx context.Context, id string, resource *ProtobufWithMetadata) error {
	panic("not implemented")
}

/*
Upserts a resource into DB. Sets overall status for the resource to PENDING.
Upsert is going to be rejected if this is an update of an existing resource and if the revision is out-of-date.
*/
func (r *RealizationStore) Upsert(ctx context.Context, resource ProtobufWithMetadata) error {
	panic("not implemented")
}

/*
Sets enforcement status as PENDING for the resources at the given enforcement point.
Sets overall status as PENDING for these resources.

Operation fails if the revision(s) is/are out-of-date.
*/
func (r *RealizationStore) MarkEnforcementAsPending(ctx context.Context, enforcementPoint string, resources ...ProtobufWithMetadata) error {
	panic("not implemented")
}

/*
Sets enforcement status as REALIZED for the resources at the given enforcement point.
Sets overall status for a resource as REALIZED if it has been realized successfully at all enforcement points.

Operation fails if the revision(s) is/are out-of-date.
*/
func (r *RealizationStore) MarkEnforcementAsSuccess(ctx context.Context, enforcementPoint string, resources ...ProtobufWithMetadata) error {
	panic("not implemented")
}

/*
Sets enforcement status as ERROR for the resources at the given enforcement point.
Sets overall status as ERROR for these resources.

Operation fails if the revision(s) is/are out-of-date.
*/
func (r *RealizationStore) MarkEnforcementAsError(ctx context.Context, enforcementPoint string, error string, resources ...ProtobufWithMetadata) error {
	panic("not implemented")
}

/*
Deletes a resource from the DB. Sets overall status as DELETION_PENDING.

Operation fails if the revision(s) is/are out-of-date.
*/
func (r *RealizationStore) Delete(ctx context.Context, resource ProtobufWithMetadata) error {
	panic("not implemented")
}

/*
Sets enforcement status as DELETION_PENDING for the resources at the given enforcement point.

Operation fails if the revision(s) is/are out-of-date.
*/
func (r *RealizationStore) MarkEnforcementAsDeletionPending(ctx context.Context, enforcementPoint string, resources ...ProtobufWithMetadata) error {
	panic("not implemented")
}

/*
Sets enforcement status as DELETION_REALIZED for the resources at the given enforcement point.
Sets overall status for a resource as DELETION_REALIZED if it has been deleted successfully from all enforcement points.

Operation fails if the revision(s) is/are out-of-date.
*/
func (r *RealizationStore) MarkEnforcementAsDeletionRealized(ctx context.Context, enforcementPoint string, resources ...ProtobufWithMetadata) error {
	panic("not implemented")
}

/*
Returns overall status of a resource.
*/
func (r *RealizationStore) GetOverallStatus(ctx context.Context, resource ProtobufWithMetadata) (Status, error) {
	panic("not implemented")
}

/*
Returns overall status of a resource, along with the statuses at all enforcement points.
*/
func (r *RealizationStore) GetOverallStatusWithEnforcementDetails(ctx context.Context, resource ProtobufWithMetadata) (
	Status, //Overall status
	map[string]Status, //Key - enforcement point identifier; value - enforcement status
	error) {
	panic("not implemented")
}
