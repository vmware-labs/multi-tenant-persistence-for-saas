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

package realization_store

//go:generate stringer -type=Status
type Status int

// The values are ordered in th priority order, where the highest
// value is set as overall status for given resource, from the
// list of enforcement statuses for that resource
// NOTE: Do not reorder or change the values of the statuses.
const (
	UNKNOWN              Status = 0
	DELETION_REALIZED    Status = 100
	REALIZED             Status = 200
	DELETION_IN_PROGRESS Status = 300
	DELETION_PENDING     Status = 400
	IN_PROGRESS          Status = 600
	PENDING              Status = 800
	ERROR                Status = 1000
)
