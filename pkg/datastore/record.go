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

import "reflect"

type Record interface {
	// Currently are Records are expected to be pointers to struct, this is
	// just place holder for future support
}

func GetRecordInstanceFromSlice(x interface{}) Record {
	sliceType := reflect.TypeOf(x)
	if sliceType.Kind() == reflect.Ptr {
		sliceType = sliceType.Elem()
	}

	// True if x consists of pointers to structs
	var areSliceElemPtrs bool = sliceType.Elem().Kind() == reflect.Ptr

	sliceElemType := sliceType.Elem()
	if sliceElemType.Kind() == reflect.Ptr {
		sliceElemType = sliceElemType.Elem()
	}

	var record Record
	if areSliceElemPtrs {
		record = reflect.New(sliceElemType).Interface().(Record)
	} else {
		record = reflect.New(sliceElemType).Elem().Interface().(Record)
	}
	return record
}
