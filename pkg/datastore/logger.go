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
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

const (
	// Logging configuration variables.
	LOG_LEVEL_ENV_VAR = "LOG_LEVEL"

	// Constants for LOG field names & values.
	COMP             = "comp"
	SAAS_PERSISTENCE = "persistence"
)

func GetLogger() *logrus.Logger {
	log := logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05.000",
	})
	loglevel := strings.ToLower(os.Getenv(LOG_LEVEL_ENV_VAR))
	if level, err := logrus.ParseLevel(loglevel); err != nil {
		log.SetLevel(logrus.InfoLevel) // Default logging level
	} else {
		log.SetLevel(level)
	}
	return log
}

func GetCompLogger() *logrus.Entry {
	return GetLogger().WithField(COMP, SAAS_PERSISTENCE)
}
