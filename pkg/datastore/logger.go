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
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm/logger"
)

const (
	// Logging configuration variables.
	LOG_LEVEL_ENV_VAR = "LOG_LEVEL"

	// Constants for LOG field names & values.
	COMP             = "comp"
	SAAS_PERSISTENCE = "persistence"
)

var TRACE = func(format string, v ...any) {
}

func logTrace(format string, v ...any) {
	GetCompLogger().Tracef(format, v...)
}

func GetLogLevel() string {
	return strings.ToLower(os.Getenv(LOG_LEVEL_ENV_VAR))
}

func GetLogger() *logrus.Logger {
	log := logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05.000",
	})
	loglevel := GetLogLevel()
	if loglevel == "trace" {
		log.SetLevel(logrus.TraceLevel)
		TRACE = logTrace
	} else if level, err := logrus.ParseLevel(loglevel); err != nil {
		log.SetLevel(logrus.InfoLevel) // Default logging level
	} else {
		log.SetLevel(level)
	}
	return log
}

func GetGormLogger(l *logrus.Entry) logger.Interface {
	loglevel := GetLogLevel()
	if loglevel == "trace" || loglevel == "debug" {
		return logger.Default.LogMode(logger.Info)
	}
	_ = gormLogger{log: l}
	return logger.Default.LogMode(logger.Silent)
}

func GetCompLogger() *logrus.Entry {
	return GetLogger().WithField(COMP, SAAS_PERSISTENCE)
}

type gormLogger struct {
	log *logrus.Entry
}

func (l gormLogger) LogMode(level logger.LogLevel) logger.Interface {
	newlogger := l
	return newlogger
}

func (l gormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	l.log.WithContext(ctx).Infof(msg, data...)
}

// Warn print warn messages.
func (l gormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	l.log.WithContext(ctx).Warnf(msg, data...)
}

// Error print error messages.
func (l gormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	l.log.WithContext(ctx).Errorf(msg, data...)
}

func (l gormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	// TBD
}
