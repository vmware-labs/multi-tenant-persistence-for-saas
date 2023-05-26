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

package errors

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

type ErrorContextKey string

const (
	ENV_VAR           = ErrorContextKey("EnvVar")
	VALUE             = ErrorContextKey("Value")
	DB_ADMIN_USERNAME = ErrorContextKey("dbAdminUsername")
	DB_HOST           = ErrorContextKey("dbHost")
	DB_NAME           = ErrorContextKey("dbName")
	DB_PORT           = ErrorContextKey("dbPort")
	DB_USERNAME       = ErrorContextKey("dbUsername")
	PORT_NUMBER       = ErrorContextKey("PortNumber")
	SQL_STMT          = ErrorContextKey("SqlStmt")
	SSL_MODE          = ErrorContextKey("sslMode")
	TABLE_NAME        = ErrorContextKey("TableName")
	TYPE              = ErrorContextKey("Type")
	DB_ROLE           = ErrorContextKey("DbRole")
)

func contextToString(ctx context.Context) string {
	contextType := reflect.TypeOf(ctx).Elem()
	if contextType.Kind() != reflect.Struct {
		return ""
	}

	contextValue := reflect.ValueOf(ctx).Elem()
	numFields := contextValue.NumField()
	ctxStrings := make([]string, 0, numFields)
	for i := 0; i < numFields; i++ {
		reflectValue := contextValue.Field(i)
		reflectValue = reflect.NewAt(reflectValue.Type(), unsafe.Pointer(reflectValue.UnsafeAddr())).Elem()

		reflectField := contextType.Field(i)
		switch reflectField.Name {
		case "Context":
			innerCtxStr := contextToString(reflectValue.Interface().(context.Context))
			if len(innerCtxStr) > 0 {
				ctxStrings = append(ctxStrings, innerCtxStr+", ")
			}
		case "key":
			keyStr := fmt.Sprintf("%+v=", reflectValue.Interface())
			ctxStrings = append(ctxStrings, keyStr)
		case "val":
			valueStr := fmt.Sprintf("%+v", reflectValue.Interface())
			ctxStrings = append(ctxStrings, valueStr)
		default:
			str := fmt.Sprintf("%+v=%+v,", reflectField.Name, reflectValue.Interface())
			ctxStrings = append(ctxStrings, str)
		}
	}
	return strings.Join(ctxStrings, "")
}

type DbError struct {
	msg string
	err error
	ctx context.Context
}

func (e *DbError) Error() string {
	var errStrBuilder strings.Builder

	// Add error's own message
	errStrBuilder.WriteString(e.msg)

	// Add error's key-value pairs
	if e.ctx != nil {
		errStrBuilder.WriteString(" [")
		errStrBuilder.WriteString(contextToString(e.ctx))
		errStrBuilder.WriteRune(']')
	}

	// Add nested error on a new line
	if e.err != nil {
		errStrBuilder.WriteRune('\n')
		errStrBuilder.WriteString(e.err.Error())
	}

	// Indent each nested error
	errStr := errStrBuilder.String()
	if split := strings.Split(errStr, "\n"); len(split) > 1 {
		errStrBuilder.Reset()
		for i := range split {
			split[i] = strings.TrimSpace(split[i])
			for tabsPrinted := 0; tabsPrinted < i; tabsPrinted++ {
				split[i] = "\t" + split[i]
			}
		}
		errStr = strings.Join(split, "\n")
	}

	return errStr
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
	ctx := e.ctx
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
	ctx := e.ctx
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
	ErrBaseDb              = &DbError{}
	ErrMissingRoleMapping  = ErrBaseDb.With("Missing role mapping for DB table")
	ErrNotPtrToStructSlice = ErrBaseDb.With("Argument is invalid")
	ErrInvalidPortNumber   = ErrBaseDb.With("Port # is invalid")
	ErrMissingEnvVar       = ErrBaseDb.With("An environment variable is missing or empty")
	ErrMissingOrgId        = ErrBaseDb.With("OrgId is missing in context")
	ErrConnectingToDb      = ErrBaseDb.With("Failed to establish a connection with database")
	ErrExecutingSqlStmt    = ErrBaseDb.With("SQL statement could not be executed")
	ErrRegisteringStruct   = ErrBaseDb.With("Registration of a struct with DAL failed")
	ErrStartingTx          = ErrBaseDb.With("Failed to start a transaction")
	ErrCommittingTx        = ErrBaseDb.With("Failed to commit a transaction")
	ErrRevisionConflict    = ErrBaseDb.With("Blocking update due to outdated revision")
	ErrMarshalling         = ErrBaseDb.With("Cannot marshal proto message to binary")
	ErrUnmarshalling       = ErrBaseDb.With("Cannot unmarshal binary to proto message")
	ErrOperationNotAllowed = ErrBaseDb.With("Not authorized to perform the operation on other's data")
	ErrFetchingMetadata    = ErrBaseDb.With("Error fetching metadata from GRPC context")
	ErrTableDoesNotExist   = ErrBaseDb.With("Table does not exist")
	ErrRecordNotFound      = ErrBaseDb.With("Unable to locate record")
	ErrNotPtrToStruct      = ErrBaseDb.With("PointerToStruct expected, invalid type provided")

	ErrAuthContext       = ErrBaseDb.With("Error extracting authContext from context")
	ErrNoAuthContext     = ErrBaseDb.With("Permission denied because authContext is missing")
	ErrNoUserContext     = ErrBaseDb.With("Permission denied because userInformation is missing")
	ErrUserNotAuthorized = ErrBaseDb.With("User is not authorized to access this API")
	ErrMissingInstanceId = ErrBaseDb.With("Instance ID is not configured in the context")
)
