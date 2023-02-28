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

const (
	DEFAULT_OFFSET = 0
	DEFAULT_LIMIT  = 1000
	DEFAULT_SORTBY = ""
)

type Pagination struct {
	Offset int
	Limit  int
	SortBy string
}

func GetPagination(offset int, limit int, sortBy string) *Pagination {
	return &Pagination{
		Offset: offset,
		Limit:  limit,
		SortBy: sortBy,
	}
}

func DefaultPagination() *Pagination {
	return GetPagination(DEFAULT_OFFSET, DEFAULT_LIMIT, DEFAULT_SORTBY)
}

func NoPagination() *Pagination {
	return nil
}
