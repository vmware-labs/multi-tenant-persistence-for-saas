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

// This package exposes IRealizationStore interface that will be used to support managing statuses of resources being realized at
// different enforcement points and aggregating it OverallStatus to represent the state of resource.
//
// ## Implementation
//
// Two database tables will be used to track realization status of each resource
// type. The first one, `EnforcementStatus`, stores realization status of a resource at specific enforcement points, where an enforcement
// point is a workload where a resource is actually realized and where its status can be queried. The second one, `OverallStatus`, stores
// overall realization status of a resource across all enforcement points. The following ER diagrams show the DB schema for resource as an
// example.
//
// ### ER diagram
//
// ```mermaid erDiagram
//
//	Resource ||--|| ResourceOverallStatus : "Overall Status"
//	Resource ||--o{ ResourceEnforcementStatus : "Status Per Enforcement Point"
//
//	Resource {
//	    string id
//	    string org_id
//	    byte[] msg
//	    string parent_id
//	    bigint revision
//	}
//
//	ResourceOverallStatus {
//	    string id
//	    string org_id
//	    enum status "DELETION_REALIZED|REALIZED|DELETION_IN_PROGRESS|DELETION_PENDING|IN_PROGRESS|PENDING|ERROR"
//	    string additional_details
//	    created_at timestamptz
//	    updated_at timestamptz
//	    bigint revision
//	}
//
//	ResourceEnforcementStatus {
//	    string id
//	    string org_id
//	    string enforcement_point_id
//	    enum status  "DELETION_REALIZED|REALIZED|DELETION_IN_PROGRESS|DELETION_PENDING|IN_PROGRESS|PENDING|ERROR"
//	    string additional_details
//	    created_at timestamptz
//	    updated_at timestamptz
//	    bigint revision
//	}
//
// ```
//
// ## Common workflows
//
// The common workflows related to realization status support would be these:
// **Scenario: A resource is created**
// End user issues a request to persist an intent for resource _R1_. It needs to be realized at enforcement points _E1_ & _E2_. Consumer(s) would call
// these methods:
//
//	PersistIntent(R1)
//	MarkEnforcementAsPending(E1, R1)
//	MarkEnforcementAsPending(E2, R1)
//	//R1 is successfully realized at E1
//	MarkEnforcementAsSuccess(E1, R1)
//	//R1 is successfully realized at E2
//	MarkEnforcementAsSuccess(E2, R1)
//
// **Scenario: A resource is modified. Its status is queried**
// End user issues a request to perist a modified intent for resource _R1_. The modified resource needs to be realized at enforcement points _E1_ & _E2_.
// Consumer(s) would call these methods:
//
//	PersistIntent(R1)
//	MarkEnforcementAsPending(E1, R1)
//	MarkEnforcementAsPending(E2, R1)
//	//R1 is successfully realized at E1
//	MarkEnforcementAsSuccess(E1, R1)
//	//R1 is successfully realized at E2
//	MarkEnforcementAsSuccess(E2, R1)
//	GetOverallStatusWithEnforcementDetails(R1)
//
// **Scenario: A resource is deleted**
// End user issues a request to delete an intent for resource _R1_. The deleted resource needs to be "unenforced" at enforcement points _E1_ & _E2_.
// Consumer(s) would call these methods:
//
//	Delete(R1)
//	MarkEnforcementAsDeletionPending(E1, R1)
//	MarkEnforcementAsDeletionPending(E1, R1)
//	//R1 is successfully "unenforced" at E1
//	MarkEnforcementAsDeletionRealized(E1, R1) //TBD
//	//R1 is successfully "unenforced" at E2
//	MarkEnforcementAsDeletionRealized(E2, R1) //TBD
//	//Cleanup mechanism deletes stale status records for deleted resources - TBD
//
// **Scenario: A resource is created but fails to be realized at some enforcement points**
// End user issues a request to persist an intent for resource _R1_. It needs to be realized at enforcement points _E1_ & _E2_. Consumer(s) would call
// these methods:
//
//	PersistIntent(R1)
//	MarkEnforcementAsPending(E1, R1)
//	MarkEnforcementAsPending(E2, R1)
//	//R1 fails to be realized at E1 due to error err
//	MarkEnforcementAsError(E1, err, R1)
//	//R1 is successfully realized at E2
//	MarkEnforcementAsSuccess(E2, R1)
//
// **Scenario: A new enforcement point is added**
// An enforcement point _E3_ is added (by end-user or an admin). Resources _R1_ and _R2_ need to be enforced on it. Consumer(s) would call
// these methods:
//
//	//Enforcement point is added
//	MarkEnforcementAsPending(E3, R1, R2)
//	//R1 is successfully realized at E3
//	MarkEnforcementAsSuccess(E3, R1)
//	//R2 is successfully realized at E3
//	MarkEnforcementAsSuccess(E3, R2)
//
// **Scenario: An enforcement point is removed**
// An enforcement point _E3_ is added (by end-user or an admin). Resources _R1_ and _R2_ need to be "unenforced" on it. Consumer(s) would call
// these methods:
//
//	//Enforcement point is removed
//	MarkEnforcementAsDeletionRealized(E3, R1, R2) //TBD
package realization_store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/protostore"
)

const (
	ADDITIONAL_DETAILS_LENGTH_CAP = 1024
)

type IRealizationStore interface {
	Register(ctx context.Context, roleMapping map[string]dbrole.DbRole, msgs ...proto.Message) error

	FindById(ctx context.Context, id string, resource *ProtobufWithMetadata) error

	PersistIntent(ctx context.Context, resource *ProtobufWithMetadata) (rowsAffected int64, metadata protostore.Metadata, err error)
	MarkEnforcementAsPending(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error
	MarkEnforcementAsInProgress(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error
	MarkEnforcementAsSuccess(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error
	MarkEnforcementAsError(ctx context.Context, enforcementPoint string, errStr string, resources ...*ProtobufWithMetadata) error

	SoftDelete(ctx context.Context, resource *ProtobufWithMetadata) (rowsAffected int64, metadata protostore.Metadata, err error)
	Delete(ctx context.Context, resource *ProtobufWithMetadata) (rowsAffected int64, err error)
	Purge(ctx context.Context, resource *ProtobufWithMetadata) (rowsAffected int64, err error)
	MarkEnforcementAsDeletionPending(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error
	MarkEnforcementAsDeletionInProgress(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error
	MarkEnforcementAsDeletionRealized(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error
	MarkEnforcementAsDeletionError(ctx context.Context, enforcementPoint string, errStr string, resources ...*ProtobufWithMetadata) error

	GetOverallStatus(ctx context.Context, resource *ProtobufWithMetadata) (Status, error)
	GetOverallStatusWithEnforcementDetails(ctx context.Context, resource *ProtobufWithMetadata) (Status, map[string]Status, error)
	GetEnforcementStatusMap(ctx context.Context, resource *ProtobufWithMetadata) (overallStatus OverallStatus, enforcementStatusMap map[string]EnforcementStatus, err error)

	// To handle error scenarios and for intrumentation cases
	PurgeStaleStatusRecords(ctx context.Context, resource *ProtobufWithMetadata) (rowsAffected int64, err error)
	PurgeStaleEnforcementStatusRecords(ctx context.Context, resource *ProtobufWithMetadata, enforcementStatusRecordMap map[string]EnforcementStatus) (rowsAffected int64, err error)
}

type realizationStoreV0 struct {
	dataStore  datastore.DataStore
	protoStore protostore.ProtoStore
	logger     *logrus.Entry
}

/*
Registers the given Protobuf messages with DAL, along with their overall status and enforcement status records.
*/
func (r *realizationStoreV0) Register(ctx context.Context, roleMapping map[string]dbrole.DbRole, msgs ...proto.Message) error {
	for _, msg := range msgs {
		resourceName := datastore.GetTableName(msg)
		logger := r.logger.WithField(RESOURCE, resourceName)
		logger.Debugln(REGISTERING, RESOURCE, STARTED)
		err := r.protoStore.Register(ctx, roleMapping, msg)
		if err != nil {
			err = ErrRegisteringStruct.Wrap(err).WithValue(RESOURCE, resourceName)
			logger.Errorln(REGISTERING, RESOURCE, FAILED, err)
			return err
		}

		// OVERALL STATUS
		overallStatusTableName := GetOverallStatusTableName(msg)
		err = r.dataStore.Helper().RegisterHelper(ctx, roleMapping, overallStatusTableName, OverallStatus{})
		if err != nil {
			err = ErrRegisteringStruct.Wrap(err).WithValue(RESOURCE, resourceName)
			logger.Errorln(REGISTERING, RESOURCE, FAILED, err)
			return err
		}

		// ENFORCEMENT STATUS
		enforcementStatusTableName := GetEnforcementStatusTableName(msg)
		err = r.dataStore.Helper().RegisterHelper(ctx, roleMapping, enforcementStatusTableName, EnforcementStatus{})
		if err != nil {
			err = ErrRegisteringStruct.Wrap(err).WithValue(RESOURCE, resourceName)
			logger.Errorln(REGISTERING, RESOURCE, FAILED, err)
			return err
		}
		logger.Infoln(REGISTERING, RESOURCE, FINISHED)
	}

	return nil
}

/*
Checks if a resource with the given ID and revision exists under the user's org.
*/
func (r *realizationStoreV0) doesResourceExist(ctx context.Context, resource *ProtobufWithMetadata) bool {
	queryResults := &ProtobufWithMetadata{resource.Message, protostore.Metadata{}}
	if err := r.FindById(ctx, resource.Id, queryResults); err != nil {
		return false
	}

	return queryResults.Id == resource.Id && queryResults.Revision == resource.Revision
}

/*
Finds a resource by ID.
*/
func (r *realizationStoreV0) FindById(ctx context.Context, id string, queryResults *ProtobufWithMetadata) error {
	err := r.protoStore.FindById(ctx, id, queryResults.Message, &queryResults.Metadata)
	if err != nil {
		return err
	}

	return nil
}

/*
Persists(Upserts) the intent for a resource into DB. Intent for a resource is configuration provided by consumer, but not yet fully
realized at the expected enforcement points as required by the business logic. Sets overall status for the resource to PENDING.
Deletes any stale overall/enforcement status records of the resource that has been previously deleted and whose ID is now being reused.

PersistIntent is going to be rejected if this is an update of an existing resource and if the revision is out-of-date.
*/
func (r *realizationStoreV0) PersistIntent(ctx context.Context, resource *ProtobufWithMetadata) (rowsAffected int64, metadata protostore.Metadata, err error) {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return 0, protostore.Metadata{}, err
	}
	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id})
	logger.Debugln(PERSISTING, INTENT, STARTED)

	// Delete stale records in overall & enforcement status DB tables, if there are any
	numStaleRecordsDeleted, err := r.PurgeStaleStatusRecords(ctx, resource)
	if err != nil {
		logger.Errorln(PERSISTING, INTENT, FAILED, err)
		return 0, protostore.Metadata{}, err
	} else if numStaleRecordsDeleted > 0 {
		logger.Warnf("%d stale records have been deleted. Previously deleted resource's ID must have been reused", numStaleRecordsDeleted)
	}

	// Upsert new resource
	rowsAffected, metadata, err = r.protoStore.UpsertWithMetadata(ctx, resource.Id, resource.Message, resource.Metadata)
	if err != nil {
		logger.Errorln(PERSISTING, RESOURCE, FAILED, err)
		return 0, protostore.Metadata{}, err
	}

	resource.Metadata.Revision = metadata.Revision
	if err = r.resetAllEnforcementStatus(ctx, resource, PENDING); err != nil {
		logger.Errorln(PERSISTING, INTENT, FAILED, err)
		return 0, protostore.Metadata{}, err
	}
	// In single transaction this computation can be skipped here and we can setOverallStatus.
	if err = r.computeOverallStatus(ctx, resource, PENDING); err != nil {
		logger.Errorln(PERSISTING, INTENT, FAILED, err)
		return 0, protostore.Metadata{}, err
	}

	logger.Infoln(PERSISTING, INTENT, FINISHED)
	return rowsAffected, metadata, nil
}

/*
Sets enforcement status as PENDING for the resources at the given enforcement point.
Operation fails if any resource's revision is out-of-date.
*/
func (r *realizationStoreV0) MarkEnforcementAsPending(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error {
	for i := range resources {
		resource := resources[i]
		if err := r.markEnforcementStatus(ctx, enforcementPoint, resource, PENDING); err != nil {
			return err
		}
	}
	return nil
}

/*
Sets enforcement status as IN_PROGRESS for the resources at the given enforcement point.
Operation fails if any resource's revision is out-of-date.
*/
func (r *realizationStoreV0) MarkEnforcementAsInProgress(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error {
	for i := range resources {
		resource := resources[i]
		if err := r.markEnforcementStatus(ctx, enforcementPoint, resource, IN_PROGRESS); err != nil {
			return err
		}
	}
	return nil
}

/*
Sets enforcement status as REALIZED for the resources at the given enforcement point.
Sets overall status for a resource as REALIZED if it has been realized successfully at all enforcement points.

Operation fails if any resource's revision is out-of-date.
*/
func (r *realizationStoreV0) MarkEnforcementAsSuccess(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error {
	for i := range resources {
		resource := resources[i]
		if err := r.markEnforcementStatus(ctx, enforcementPoint, resource, REALIZED); err != nil {
			return err
		}
	}
	return nil
}

/*
Sets enforcement status for the given resource at the given enforcement point.
Operation fails if resource's revision is out-of-date.
*/
func (r *realizationStoreV0) markEnforcementStatus(ctx context.Context, enforcementPoint string, resource *ProtobufWithMetadata, status Status) error {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return err
	}

	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id, ENFORCEMENT_POINT: enforcementPoint})
	logger.Debugln(MARKING, ENFORCEMENT, status.String(), STARTED)

	if !r.doesResourceExist(ctx, resource) {
		err = ErrRecordNotFound.WithValue("Revision", strconv.FormatInt(resource.Revision, 10))
		err = ErrMarkingEnforcementFailed.WithValue("status", status.String()).Wrap(err)
		logger.Errorln(MARKING, ENFORCEMENT, FAILED, err)
		return err
	}

	// Insert/update enforcement status record
	if err = r.setEnforcementStatus(ctx, resource, enforcementPoint, status, ""); err != nil {
		err = ErrMarkingEnforcementFailed.WithValue("status", status.String()).Wrap(err)
		logger.Errorln(MARKING, ENFORCEMENT, FAILED, err)
		return err
	}

	if err = r.computeOverallStatus(ctx, resource, PENDING); err != nil {
		logger.Errorln(MARKING, ENFORCEMENT, status.String(), FAILED, err)
		return err
	}

	logger.Infoln(MARKING, ENFORCEMENT, status.String(), FINISHED)
	return nil
}

/*
Sets enforcement status as ERROR for the resources at the given enforcement point.
Sets overall status as ERROR for these resources.

Operation fails if any resource's revision is out-of-date.
//TODO indicate the most recently successful enforcement in enforcement status record in addition to overall status record?
*/
func (r *realizationStoreV0) MarkEnforcementAsError(ctx context.Context, enforcementPoint string, errStr string, resources ...*ProtobufWithMetadata) error {
	for i := range resources {
		resource := resources[i]
		if err := r.markEnforcementAsError(ctx, enforcementPoint, errStr, false, resource); err != nil {
			return err
		}
	}
	return nil
}

func (r *realizationStoreV0) MarkEnforcementAsDeletionError(ctx context.Context, enforcementPoint string, errStr string, resources ...*ProtobufWithMetadata) error {
	for i := range resources {
		resource := resources[i]
		if err := r.markEnforcementAsError(ctx, enforcementPoint, errStr, true, resource); err != nil {
			return err
		}
	}
	return nil
}

/*
Sets enforcement status as ERROR for the given resource at the given enforcement point.
Sets overall status as ERROR for this resource.

Operation fails if resource's revision is out-of-date.

TODO consider prepending to additional_details column and capping its length using a SQL statement rather than through Golang.
*/
func (r *realizationStoreV0) markEnforcementAsError(ctx context.Context, enforcementPoint string, errStr string, resourceDeleted bool, resource *ProtobufWithMetadata) error {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return err
	}

	status := ERROR
	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id, ENFORCEMENT_POINT: enforcementPoint})
	logger.Debugln(MARKING, ENFORCEMENT, status.String(), STARTED)

	if !r.doesResourceExist(ctx, resource) && !resourceDeleted {
		err = ErrRecordNotFound.WithValue("Revision", strconv.FormatInt(resource.Revision, 10))
		err = ErrMarkingEnforcementFailed.WithValue("status", status.String()).Wrap(err)
		logger.Errorln(MARKING, ENFORCEMENT, status.String(), FAILED, err)
		return err
	}

	// Change enforcement status to error
	enforcementStatusRecord := GetModelEnforcementStatusRecord(resource, orgId)
	enforcementStatusRecord.EnforcementPointId = enforcementPoint
	if resourceDeleted {
		_ = r.dataStore.FindSoftDeleted(ctx, enforcementStatusRecord)
	} else {
		_ = r.dataStore.Find(ctx, enforcementStatusRecord)
	}
	additionalDetails := fmt.Sprintf("ERROR %s at revision %d; %s", errStr, resource.Revision, enforcementStatusRecord.AdditionalDetails)
	if len(additionalDetails) > ADDITIONAL_DETAILS_LENGTH_CAP {
		additionalDetails = additionalDetails[:ADDITIONAL_DETAILS_LENGTH_CAP]
	}
	if err = r.setEnforcementStatus(ctx, resource, enforcementPoint, ERROR, additionalDetails); err != nil {
		err = ErrMarkingEnforcementFailed.WithValue("status", status.String()).Wrap(err)
		logger.Errorln(MARKING, ENFORCEMENT, status.String(), FAILED, err)
		return err
	}

	// Change overall status to error
	overallStatusRecord := GetModelOverallStatusRecord(resource, orgId)
	if resourceDeleted {
		_ = r.dataStore.FindSoftDeleted(ctx, overallStatusRecord)
	} else {
		_ = r.dataStore.Find(ctx, overallStatusRecord)
	}
	additionalDetails = fmt.Sprintf("ERROR %s at revision %d; %s", errStr, resource.Revision, overallStatusRecord.AdditionalDetails)
	if len(additionalDetails) > ADDITIONAL_DETAILS_LENGTH_CAP {
		additionalDetails = additionalDetails[:ADDITIONAL_DETAILS_LENGTH_CAP]
	}

	if err = r.setOverallStatus(ctx, resource, ERROR, additionalDetails); err != nil {
		err = ErrMarkingEnforcementFailed.WithValue("status", status.String()).Wrap(err)
		logger.Errorln(MARKING, ENFORCEMENT, status.String(), FAILED, err)
	}

	logger.Infoln(MARKING, ENFORCEMENT, status.String(), FINISHED)
	return nil
}

/*
Soft Deletes a resource from the DB. Sets overall status and enforcement status to DELETION_PENDING.
*/
func (r *realizationStoreV0) SoftDelete(ctx context.Context, resource *ProtobufWithMetadata) (rowsAffected int64, metadata protostore.Metadata, err error) {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return 0, protostore.Metadata{}, err
	}

	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id})

	logger.Debugln(SOFT_DELETING, RESOURCE, STARTED)
	if rowsAffected, metadata, err = r.protoStore.SoftDeleteById(ctx, resource.Id, resource.Message); err != nil {
		logger.Errorln(SOFT_DELETING, RESOURCE, FAILED, err)
		return 0, protostore.Metadata{}, err
	} else if rowsAffected == 0 {
		err = ErrRecordNotFound.WithValue("Revision", strconv.FormatInt(resource.Revision, 10))
		logger.Warnln(err)
	}
	resource.Metadata.Revision = metadata.Revision

	if err = r.resetAllEnforcementStatus(ctx, resource, DELETION_PENDING); err != nil {
		logger.Errorln(SOFT_DELETING, RESOURCE, FAILED, err)
		return 0, protostore.Metadata{}, err
	}

	// In single transaction this computation can be skipped here and we can setOverallStatus.
	if err = r.computeOverallStatus(ctx, resource, DELETION_PENDING); err != nil {
		logger.Errorln(SOFT_DELETING, RESOURCE, FAILED, err)
		return 0, protostore.Metadata{}, err
	}

	logger.Infoln(SOFT_DELETING, RESOURCE, FINISHED)
	return rowsAffected, metadata, nil
}

/*
Deletes a resource from the DB. Sets overall status as DELETION_PENDING.
*/
func (r *realizationStoreV0) Delete(ctx context.Context, resource *ProtobufWithMetadata) (rowsAffected int64, err error) {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return 0, err
	}

	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id})

	logger.Debugln(DELETING, RESOURCE, STARTED)
	if rowsAffected, err = r.protoStore.DeleteById(ctx, resource.Id, resource.Message); err != nil {
		logger.Errorln(DELETING, RESOURCE, FAILED, err)
		return 0, err
	} else if rowsAffected == 0 {
		err = ErrRecordNotFound.WithValue("Revision", strconv.FormatInt(resource.Revision, 10))
		logger.Warnln(err)
	}
	logger.Infoln(DELETING, RESOURCE, FINISHED)

	// OverallStatus is marked as DELETION_REALIZED if there are no enforcement records
	if err = r.computeOverallStatus(ctx, resource, DELETION_REALIZED); err != nil {
		return rowsAffected, err
	}

	return rowsAffected, nil
}

/*
Purges a resource along with overall and enforcement status from the DB.
*/
func (r *realizationStoreV0) Purge(ctx context.Context, resource *ProtobufWithMetadata) (rowsAffected int64, err error) {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return 0, err
	}

	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id})
	var tx *gorm.DB

	tableName := GetEnforcementStatusTableName(resource.Message)
	filter := GetModelEnforcementStatusRecordWithoutRevision(resource, orgId)

	logger.Debugln(PURGING, RESOURCE, STARTED)
	recordsDeleted, err := r.protoStore.DeleteById(ctx, resource.Id, resource.Message)
	rowsAffected += recordsDeleted
	if err != nil && err != ErrRecordNotFound {
		logger.Errorln(PURGING, RESOURCE, FAILED, err)
		return rowsAffected, err
	}

	recordsDeleted, err = r.deleteOverallStatus(ctx, resource)
	rowsAffected += recordsDeleted
	if err != nil && err != ErrRecordNotFound {
		logger.Errorln(PURGING, RESOURCE, FAILED, err)
		return rowsAffected, err
	}

	if tx, err = r.dataStore.Helper().GetDBTransaction(ctx, tableName, filter); err != nil {
		rowsAffected += tx.RowsAffected
		logger.Errorln(PURGING, RESOURCE, FAILED, err)
		return rowsAffected, err
	}
	defer rollbackTx(ctx, tx, logger)

	if err = tx.Table(tableName).Where(filter).Delete(filter).Error; err != nil {
		rowsAffected += tx.RowsAffected
		return rowsAffected, err
	}
	logger.Infoln(PURGING, RESOURCE, FINISHED)

	return rowsAffected, nil
}

/*
Sets enforcement status as DELETION_PENDING for the resources at the given enforcement point.
*/
func (r *realizationStoreV0) MarkEnforcementAsDeletionPending(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error {
	for i := range resources {
		resource := resources[i]
		if err := r.markEnforcementDeletionStatus(ctx, enforcementPoint, resource, DELETION_PENDING); err != nil {
			return err
		}
	}
	return nil
}

/*
Sets enforcement status as DELETION_IN_PROGRESS for the resources at the given enforcement point.
*/
func (r *realizationStoreV0) MarkEnforcementAsDeletionInProgress(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error {
	for i := range resources {
		resource := resources[i]
		if err := r.markEnforcementDeletionStatus(ctx, enforcementPoint, resource, DELETION_IN_PROGRESS); err != nil {
			return err
		}
	}
	return nil
}

/*
Sets enforcement status as DELETION_REALIZED for the resources at the given enforcement point.
*/
func (r *realizationStoreV0) MarkEnforcementAsDeletionRealized(ctx context.Context, enforcementPoint string, resources ...*ProtobufWithMetadata) error {
	for i := range resources {
		resource := resources[i]
		if err := r.markEnforcementDeletionStatus(ctx, enforcementPoint, resource, DELETION_REALIZED); err != nil {
			return err
		}
	}
	return nil
}

/*
Sets enforcement deletion status for the resource at the given enforcement point.
*/
func (r *realizationStoreV0) markEnforcementDeletionStatus(ctx context.Context, enforcementPoint string, resource *ProtobufWithMetadata, status Status) error {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return err
	}

	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id, ENFORCEMENT_POINT: enforcementPoint})
	logger.Debugln(MARKING, ENFORCEMENT, status.String(), STARTED)

	if r.doesResourceExist(ctx, resource) {
		logger.Debugln(MARKING, ENFORCEMENT, status.String(), STARTED, "[resource still exists, change in span case]")
	} else {
		logger.Debugln(MARKING, ENFORCEMENT, status.String(), STARTED)
	}

	// Change enforcement status to deletion pending
	if err = r.setEnforcementStatus(ctx, resource, enforcementPoint, status, ""); err != nil {
		err = ErrMarkingEnforcementFailed.WithValue("status", status.String()).Wrap(err)
		logger.Errorln(MARKING, ENFORCEMENT, status.String(), FAILED, err)
		return err
	}

	if err = r.computeOverallStatus(ctx, resource, DELETION_PENDING); err != nil {
		err = ErrMarkingEnforcementFailed.WithValue("status", status.String()).Wrap(err)
		logger.Errorln(MARKING, ENFORCEMENT, status.String(), FAILED, err)
		return err
	}

	logger.Infoln(MARKING, ENFORCEMENT, status.String(), FINISHED)
	return nil
}

// Computes and sets overall status to the given value.
// Overall status is set to max value of the status or defaultValue if no enforcement status records are found.
func (r *realizationStoreV0) computeOverallStatus(ctx context.Context, resource *ProtobufWithMetadata, defaultValue Status) error {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return err
	}

	// TODO: Use single SELECT query with max(status) on enforcement table
	filter := GetModelEnforcementStatusRecord(resource, orgId)
	queryResults := make([]EnforcementStatus, 0)
	err = r.dataStore.FindWithFilter(ctx, filter, &queryResults, datastore.NoPagination())
	if err != nil {
		return err
	}

	overallStatus := UNKNOWN
	for _, enforcementStatusRecord := range queryResults {
		if enforcementStatusRecord.RealizationStatus > overallStatus && enforcementStatusRecord.Revision >= resource.Revision {
			overallStatus = enforcementStatusRecord.RealizationStatus
		}
		r.logger.Traceln(overallStatus.String(), enforcementStatusRecord)
	}
	if overallStatus == UNKNOWN {
		overallStatus = defaultValue
	}
	return r.setOverallStatus(ctx, resource, overallStatus, "")
}

// Sets overall status to the given value.
func (r *realizationStoreV0) setOverallStatus(ctx context.Context, resource *ProtobufWithMetadata, realizationStatus Status, additionalDetails string) error {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return err
	}
	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id})

	logger.Debugln(SETTING, OVERALL_STATUS, STARTED)
	overallStatusRecord := GetModelOverallStatusRecord(resource, orgId)
	overallStatusRecord.RealizationStatus = realizationStatus
	overallStatusRecord.AdditionalDetails = additionalDetails
	overallStatusRecord.Revision = resource.Revision
	_, err = r.dataStore.Upsert(ctx, overallStatusRecord)
	if err != nil {
		logger.Errorln(SETTING, OVERALL_STATUS, FAILED, err)
		return err
	}

	logger.Infoln(SETTING, OVERALL_STATUS, FINISHED, realizationStatus)
	return nil
}

// Deletes the overall status for the given value.
func (r *realizationStoreV0) deleteOverallStatus(ctx context.Context, resource *ProtobufWithMetadata) (rowsAffected int64, err error) {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return 0, err
	}
	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id})

	logger.Debugln(DELETING, OVERALL_STATUS, STARTED)
	overallStatusRecord := GetModelOverallStatusRecord(resource, orgId)
	rowsAffected, err = r.dataStore.Delete(ctx, overallStatusRecord)
	if err != nil {
		logger.Errorln(DELETING, OVERALL_STATUS, FAILED, err)
		return rowsAffected, err
	}
	logger.Infoln(DELETING, OVERALL_STATUS, FINISHED)
	return rowsAffected, nil
}

func rollbackTx(ctx context.Context, tx *gorm.DB, logger *logrus.Entry) {
	txFetcher := authorizer.SimpleTransactionFetcher{}
	if !txFetcher.IsTransactionCtx(ctx) {
		if err := tx.Rollback().Error; err != nil && err != sql.ErrTxDone {
			logger.Error(err)
		}
	}
}

// Sets the realization status for all the enforcement records to given realizationStatus (used in PersistIntent/SoftDelete).
func (r *realizationStoreV0) resetAllEnforcementStatus(ctx context.Context, resource *ProtobufWithMetadata, realizationStatus Status) error {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return err
	}
	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id, ENFORCEMENT_POINT: ALL})

	tableName := GetEnforcementStatusTableName(resource.Message)
	filter := GetModelEnforcementStatusRecordWithoutRevision(resource, orgId)
	enforcementStatusRecord := GetModelEnforcementStatusRecord(resource, orgId)
	enforcementStatusRecord.RealizationStatus = realizationStatus

	logger.Debugln(RESETTING, ENFORCEMENT_STATUS, STARTED)
	var tx *gorm.DB
	if tx, err = r.dataStore.Helper().GetDBTransaction(ctx, tableName, enforcementStatusRecord); err != nil {
		return err
	}
	defer rollbackTx(ctx, tx, logger)

	if err = tx.Table(tableName).Where(filter).Updates(enforcementStatusRecord).Error; err != nil {
		logger.Error(err)
		if strings.Contains(err.Error(), datastore.REVISION_OUTDATED_MSG) {
			logger.Errorln(RESETTING, ENFORCEMENT_STATUS, FAILED, err)
			return ErrRevisionConflict.Wrap(err)
		} else {
			logger.Errorln(RESETTING, ENFORCEMENT_STATUS, FAILED, err)
			return ErrExecutingSqlStmt.Wrap(err)
		}
	}

	txFetcher := authorizer.SimpleTransactionFetcher{}
	if !txFetcher.IsTransactionCtx(ctx) {
		logger.Error("Committing the transaction for RESET")
		if err = tx.Commit().Error; err != nil {
			logger.Errorln(RESETTING, ENFORCEMENT_STATUS, FAILED, err)
			return ErrExecutingSqlStmt.Wrap(err)
		}
	}
	logger.Debugln(RESETTING, ENFORCEMENT_STATUS, FINISHED, realizationStatus.String())
	return nil
}

func (r *realizationStoreV0) setEnforcementStatus(ctx context.Context, resource *ProtobufWithMetadata,
	enforcementPoint string, realizationStatus Status, additionalDetails string,
) error {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return err
	}
	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, ENFORCEMENT_POINT: enforcementPoint, RESOURCE_ID: resource.Id})

	logger.Debugln(SETTING, ENFORCEMENT_STATUS, STARTED)
	enforcementStatusRecord := GetModelEnforcementStatusRecord(resource, orgId)
	enforcementStatusRecord.EnforcementPointId = enforcementPoint
	enforcementStatusRecord.RealizationStatus = realizationStatus
	enforcementStatusRecord.AdditionalDetails = additionalDetails
	enforcementStatusRecord.Revision = resource.Revision
	_, err = r.dataStore.Upsert(ctx, enforcementStatusRecord)
	if err != nil {
		logger.Errorln(SETTING, ENFORCEMENT_STATUS, FAILED, err)
		return err
	}

	logger.Infoln(SETTING, ENFORCEMENT_STATUS, FINISHED, realizationStatus.String())
	return nil
}

func (r *realizationStoreV0) deleteEnforcementStatus(ctx context.Context, resource *ProtobufWithMetadata, enforcementPoint string) (rowsAffected int64, err error) {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return 0, err
	}
	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id, ENFORCEMENT_POINT: enforcementPoint})
	logger.Debugln(DELETING, ENFORCEMENT_STATUS, STARTED)
	enforcementStatusRecord := GetModelEnforcementStatusRecord(resource, orgId)
	enforcementStatusRecord.EnforcementPointId = enforcementPoint
	recordsDeleted, err := r.dataStore.Delete(ctx, enforcementStatusRecord)
	rowsAffected += recordsDeleted
	if err != nil {
		logger.Errorln(DELETING, ENFORCEMENT_STATUS, FAILED, err)
		return rowsAffected, err
	}

	logger.Infoln(DELETING, ENFORCEMENT_STATUS, FINISHED)
	return rowsAffected, nil
}

/*
Returns overall status of a resource.
*/
func (r *realizationStoreV0) GetOverallStatus(ctx context.Context, resource *ProtobufWithMetadata) (Status, error) {
	overallStatusRecord, err := r.getOverallStatusRecord(ctx, resource)
	if err != nil {
		return UNKNOWN, err
	}

	return overallStatusRecord.RealizationStatus, nil
}

func (r *realizationStoreV0) getOverallStatusRecord(ctx context.Context, resource *ProtobufWithMetadata) (OverallStatus, error) {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return OverallStatus{}, err
	}
	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id})
	logger.Debugln(FETCHING, OVERALL_STATUS, STARTED)

	overallStatusRecord := GetModelOverallStatusRecord(resource, orgId)
	err = r.dataStore.Find(ctx, overallStatusRecord)
	if err != nil {
		err = ErrGettingRealizationStatus.Wrap(err)
		logger.Errorln(FETCHING, OVERALL_STATUS, FAILED, err)
		return OverallStatus{}, err
	}

	logger.Infoln(FETCHING, OVERALL_STATUS, FINISHED, overallStatusRecord.RealizationStatus.String())
	return *overallStatusRecord, nil
}

/*
Returns overall status of a resource, along with the statuses at all enforcement points.
*/
func (r *realizationStoreV0) GetOverallStatusWithEnforcementDetails(ctx context.Context, resource *ProtobufWithMetadata) (
	overallStatus Status, // Overall status
	enforcementStatusMap map[string]Status, // Key - enforcement point identifier; value - enforcement status
	err error,
) {
	overallStatusRecord, enforcementStatusRecordMap, err := r.GetEnforcementStatusMap(ctx, resource)
	if err != nil {
		return UNKNOWN, nil, err
	}

	enforcementStatusMap = make(map[string]Status)
	for enforcementPoint, enforcementStatusRecord := range enforcementStatusRecordMap {
		enforcementStatusMap[enforcementPoint] = enforcementStatusRecord.RealizationStatus
	}

	return overallStatusRecord.RealizationStatus, enforcementStatusMap, nil
}

func (r *realizationStoreV0) GetEnforcementStatusMap(ctx context.Context, resource *ProtobufWithMetadata) (
	overallStatus OverallStatus, // Overall status record
	enforcementStatusMap map[string]EnforcementStatus, // Key - enforcement point identifier; value - enforcement status record
	err error,
) {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return OverallStatus{}, nil, err
	}
	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id})

	// Get overall status
	overallStatus, err = r.getOverallStatusRecord(ctx, resource)
	if err != nil {
		return OverallStatus{}, nil, err
	}

	// Get enforcement statuses
	logger.Debugln(FETCHING, ENFORCEMENT_STATUS, STARTED)

	filter := GetModelEnforcementStatusRecordWithoutRevision(resource, orgId)
	enforcementStatuses := make([]EnforcementStatus, 0)
	err = r.dataStore.Helper().FindWithFilterInTable(
		ctx, GetEnforcementStatusTableName(resource.Message), filter, &enforcementStatuses, datastore.NoPagination(), false)
	if err != nil {
		err = ErrGettingRealizationStatus.Wrap(err)
		logger.Errorln(FETCHING, ENFORCEMENT_STATUS, FAILED, err)
		return OverallStatus{}, nil, err
	}

	enforcementStatusMap = make(map[string]EnforcementStatus, len(enforcementStatuses))
	for i := range enforcementStatuses {
		enforcementStatusMap[enforcementStatuses[i].EnforcementPointId] = enforcementStatuses[i]
	}

	logger.Infoln(FETCHING, ENFORCEMENT_STATUS, FINISHED, enforcementStatusMap)
	return overallStatus, enforcementStatusMap, nil
}

/*
Cleans up the records in the overall status and enforcement status DB tables that are related to a resource
that has already been deleted.
Returns immediately if the records are not stale (if the resource still exists).
*/
func (r *realizationStoreV0) PurgeStaleStatusRecords(ctx context.Context, resource *ProtobufWithMetadata) (rowsAffected int64, err error) {
	orgId, err := r.dataStore.GetAuthorizer().GetOrgFromContext(ctx)
	if err != nil {
		return 0, err
	}
	logger := r.logger.WithFields(logrus.Fields{ORG_ID: orgId, RESOURCE_ID: resource.Id})

	// DO NOT DELETE OVERALL/ENFORCEMENT STATUS RECORDS IF THE RESOURCE STILL EXISTS
	queryResults := new(ProtobufWithMetadata)
	queryResults.Message = proto.Clone(resource.Message)
	err = r.FindById(ctx, resource.Id, queryResults)
	if err == nil {
		logger.Tracef("Resource has not been deleted. The overall & enforcement status records are not to be purged")
		return 0, nil
	} else if err != nil && !errors.Is(err, ErrRecordNotFound) { // Error occurred when querying DB...
		logger.Error(err)
		return 0, err
	}

	overallStatusRecord, enforcementStatusRecordMap, err := r.GetEnforcementStatusMap(ctx, resource)
	if err != nil && !errors.Is(err, ErrRecordNotFound) {
		return 0, err
	}

	if overallStatusRecord.RealizationStatus == DELETION_PENDING || overallStatusRecord.RealizationStatus == DELETION_REALIZED {
		recordsDeleted, err := r.deleteOverallStatus(ctx, resource)
		rowsAffected += recordsDeleted
		if err != nil {
			return rowsAffected, err
		}
	}

	recordsDeleted, err := r.PurgeStaleEnforcementStatusRecords(ctx, resource, enforcementStatusRecordMap)
	rowsAffected += recordsDeleted
	return rowsAffected, err
}

func (r *realizationStoreV0) PurgeStaleEnforcementStatusRecords(ctx context.Context, resource *ProtobufWithMetadata, enforcementStatusRecordMap map[string]EnforcementStatus) (
	rowsAffected int64, err error,
) {
	for enforcementPoint, enforcementStatusRecord := range enforcementStatusRecordMap {
		if enforcementStatusRecord.RealizationStatus == DELETION_PENDING || enforcementStatusRecord.RealizationStatus == DELETION_REALIZED {
			recordsDeleted, err := r.deleteEnforcementStatus(ctx, resource, enforcementPoint)
			rowsAffected += recordsDeleted
			if err != nil {
				return rowsAffected, err
			}
		}
	}

	return rowsAffected, nil
}
