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
	"fmt"
	"reflect"
	"unsafe"
)

type ErrorContextKey string

const (
	ENV_VAR           = ErrorContextKey("EnvVar")
	DB_ADMIN_USERNAME = ErrorContextKey("dbAdminUsername")
	DB_HOST           = ErrorContextKey("dbHost")
	DB_NAME           = ErrorContextKey("dbName")
	DB_PORT           = ErrorContextKey("dbPort")
	DB_USERNAME       = ErrorContextKey("dbUsername")
	PORT_NUMBER       = ErrorContextKey("PortNumber")
	SQL_STMT          = ErrorContextKey("SqlStmt")
	SSL_MODE          = ErrorContextKey("sslMode")
	TABLE_NAME        = ErrorContextKey("TableName")
)

func contextToString(ctx interface{}) string {
	var ctxStr = ""
	contextValues := reflect.ValueOf(ctx).Elem()
	contextKeys := reflect.TypeOf(ctx).Elem()

	if contextKeys.Kind() == reflect.Struct {
		for i := 0; i < contextValues.NumField(); i++ {
			reflectValue := contextValues.Field(i)
			reflectValue = reflect.NewAt(reflectValue.Type(), unsafe.Pointer(reflectValue.UnsafeAddr())).Elem()

			reflectField := contextKeys.Field(i)

			if reflectField.Name == "Context" {
				ctxStr += contextToString(reflectValue.Interface())
			} else {
				if reflectField.Name == "key" {
					ctxStr += fmt.Sprintf(" %+v=", reflectValue.Interface())
				} else if reflectField.Name == "val" {
					ctxStr += fmt.Sprintf("%+v,", reflectValue.Interface())
				} else {
					ctxStr += fmt.Sprintf("%+v=%+v,", reflectField.Name, reflectValue.Interface())
				}
			}
		}
	}
	return ctxStr
}

type DbError struct {
	msg string
	err error
	ctx context.Context
}

func (e *DbError) Error() string {
	var str = e.msg
	if e.err != nil {
		str = str + ": " + e.err.Error()
	}
	if e.ctx != nil {
		str = str + "[" + contextToString(e.ctx) + "]"
	}
	return str
}

func (e *DbError) Is(target error) bool {
	if target, ok := target.(*DbError); !ok {
		return false
	} else {
		return e.msg == target.msg
	}
}

func (e *DbError) Unwrap() error {
	return e.err
}

func (e *DbError) Wrap(err error) *DbError {
	return &DbError{
		msg: e.msg,
		err: err,
		ctx: e.ctx,
	}
}

func (e *DbError) With(msg string) *DbError {
	return &DbError{
		msg: msg,
		err: e.err,
		ctx: e.ctx,
	}
}

func (e *DbError) WithContext(ctx context.Context) *DbError {
	return &DbError{
		msg: e.msg,
		err: e.err,
		ctx: ctx,
	}
}

func (c ErrorContextKey) String() string {
	return string(c)
}

func (e *DbError) WithValue(key ErrorContextKey, value string) *DbError {
	var ctx = e.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return &DbError{
		msg: e.msg,
		err: e.err,
		ctx: context.WithValue(ctx, key, value),
	}
}

func (e *DbError) WithMap(kvMap map[ErrorContextKey]string) *DbError {
	var ctx = e.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	for k, v := range kvMap {
		ctx = context.WithValue(ctx, k, v)
	}

	return &DbError{
		msg: e.msg,
		err: e.err,
		ctx: ctx,
	}
}

var (
	BaseDbError                      = &DbError{}
	ErrorMissingRoleMapping          = BaseDbError.With("Missing role mapping for DB table")
	ErrorStructNotRegisteredWithDAL  = BaseDbError.With("Struct(s) have not been registered with DAL")
	IllegalArgumentError             = BaseDbError.With("Argument is invalid")
	InvalidPortNumberError           = BaseDbError.With("Port # is invalid")
	ErrorMissingEnvVar               = BaseDbError.With("An environment variable is missing or empty")
	ErrorMissingOrgId                = BaseDbError.With("Org. ID is missing in context")
	ErrorConnectingToDb              = BaseDbError.With("Failed to establish a connection with database")
	ErrorParsingSqlRow               = BaseDbError.With("Failed to parse a row from DB table")
	ErrorExecutingSqlStmt            = BaseDbError.With("SQL statement could not be executed")
	ErrorRegisteringWithDAL          = BaseDbError.With("Registration of a struct with DAL failed")
	ErrorStartingTx                  = BaseDbError.With("Failed to start a transaction")
	ErrorCommittingTx                = BaseDbError.With("Failed to commit a transaction")
	RevisionConflictError            = BaseDbError.With("Blocking update due to outdated revision")
	ErrorMarshalling                 = BaseDbError.With("Cannot marshal proto message to binary")
	ErrorUnmarshalling               = BaseDbError.With("Cannot unmarshal binary to proto message")
	ErrOperationNotAllowed           = BaseDbError.With("Not authorized to perform the operation on other tenant's data")
	ErrorFetchingMetadataFromContext = BaseDbError.With("Error fetching metadata from GRPC context")

	ErrAuthContext       = BaseDbError.With("Error extracting authContext from context")
	ErrNoAuthContext     = BaseDbError.With("Permission denied because authContext is missing")
	ErrNoUserContext     = BaseDbError.With("Permission denied because userInformation is missing")
	ErrUserNotAuthorized = BaseDbError.With("User is not authorized to access this API")
	ErrMissingOrgId      = BaseDbError.With("Org. ID is missing in context")
)
