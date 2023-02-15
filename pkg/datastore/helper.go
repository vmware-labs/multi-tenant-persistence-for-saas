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
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	// Env. variable names.
	DB_NAME_ENV_VAR           = "DB_NAME"
	DB_PORT_ENV_VAR           = "DB_PORT"
	DB_HOST_ENV_VAR           = "DB_HOST"
	SSL_MODE_ENV_VAR          = "SSL_MODE"
	DB_ADMIN_USERNAME_ENV_VAR = "DB_ADMIN_USERNAME"
	DB_ADMIN_PASSWORD_ENV_VAR = "DB_ADMIN_PASSWORD"
)

type DBConfig struct {
	host     string
	port     int
	username string
	password string
	dbName   string
	sslMode  string
}

func FromEnv(l *logrus.Entry, authorizer authorizer.Authorizer) (d DataStore, err error) {
	// Ensure all the needed environment variables are present and non-empty
	for _, envVar := range []string{
		DB_HOST_ENV_VAR,
		DB_PORT_ENV_VAR,
		DB_ADMIN_USERNAME_ENV_VAR,
		DB_ADMIN_PASSWORD_ENV_VAR,
		DB_NAME_ENV_VAR,
		SSL_MODE_ENV_VAR,
	} {
		if _, isPresent := os.LookupEnv(envVar); !isPresent {
			err = ErrMissingEnvVar.WithValue(ENV_VAR, envVar)
			l.Error(err)
			return nil, err
		}

		if envVarValue := strings.TrimSpace(os.Getenv(envVar)); len(envVarValue) == 0 {
			err = ErrMissingEnvVar.WithValue(ENV_VAR, envVar).WithValue(VALUE, os.Getenv(envVar))
			l.Error(err)
			return nil, err
		}
	}

	var cfg DBConfig

	cfg.host = strings.TrimSpace(os.Getenv(DB_HOST_ENV_VAR))
	// Ensure port number is valid
	if cfg.port, err = strconv.Atoi(strings.TrimSpace(os.Getenv(DB_PORT_ENV_VAR))); err != nil {
		err = ErrInvalidPortNumber.Wrap(err).WithValue(ENV_VAR, DB_PORT_ENV_VAR).WithValue(VALUE, os.Getenv(DB_PORT_ENV_VAR))
		l.Error(err)
		return nil, err
	}
	cfg.username = strings.TrimSpace(os.Getenv(DB_ADMIN_USERNAME_ENV_VAR))
	cfg.password = strings.TrimSpace(os.Getenv(DB_ADMIN_PASSWORD_ENV_VAR))
	cfg.dbName = strings.TrimSpace(os.Getenv(DB_NAME_ENV_VAR))
	cfg.sslMode = strings.TrimSpace(os.Getenv(SSL_MODE_ENV_VAR))

	return FromConfig(l, authorizer, cfg)
}

func FromConfig(l *logrus.Entry, authorizer authorizer.Authorizer, cfg DBConfig) (d DataStore, err error) {
	gl := gormLogger{log: l}
	dbConnInitializer := func(db *relationalDb, dbRole dbrole.DbRole) error {
		if dbRole == dbrole.MAIN {
			// Create DB connections
			db.gormDBMap[dbrole.MAIN], err = openDb(gl, cfg.host, cfg.port, cfg.username, cfg.password, cfg.dbName, cfg.sslMode)
			if err != nil {
				args := map[ErrorContextKey]string{
					DB_HOST:           cfg.host,
					DB_PORT:           strconv.Itoa(cfg.port),
					DB_ADMIN_USERNAME: cfg.username,
					DB_NAME:           cfg.dbName,
					SSL_MODE:          cfg.sslMode,
				}
				err = ErrConnectingToDb.WithMap(args).Wrap(err)
				db.logger.Error(err)
				return err
			}

			// Create Users when the MAIN connection to DB is established
			for _, dbUserSpec := range getAllDbUsers() {
				stmt := getCreateUserStmt(string(dbUserSpec.username), dbUserSpec.password)
				if tx := db.gormDBMap[dbrole.MAIN].Exec(stmt); tx.Error != nil {
					err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt)
					db.logger.Errorln(err)
					return err
				}
			}
			return nil
		}

		dbUserSpec := getDbUser(dbRole)
		db.logger.Infof("Connecting to database %s@%s:%d[%s] ...", dbUserSpec.username, cfg.host, cfg.port, cfg.dbName)
		db.gormDBMap[dbUserSpec.username], err = openDb(gl, cfg.host, cfg.port, string(dbUserSpec.username), dbUserSpec.password, cfg.dbName, cfg.sslMode)
		if err != nil {
			args := map[ErrorContextKey]string{
				DB_HOST:     cfg.host,
				DB_PORT:     strconv.Itoa(cfg.port),
				DB_USERNAME: string(dbUserSpec.username),
				DB_NAME:     cfg.dbName,
				SSL_MODE:    cfg.sslMode,
			}
			err = ErrConnectingToDb.WithMap(args).Wrap(err)
			db.logger.Error(err)
			return err
		}
		db.logger.Infof("Connecting to database %s@%s:%d[%s] succeeded",
			dbUserSpec.username, cfg.host, cfg.port, cfg.dbName)
		if _, ok := db.gormDBMap[dbRole]; ok {
			return nil
		}

		return nil
	}
	return &relationalDb{
		authorizer:  authorizer,
		gormDBMap:   make(map[dbrole.DbRole]*gorm.DB),
		initializer: dbConnInitializer,
		logger:      l,
	}, nil
}

// Opens a Postgres DB using the provided config. parameters.
func openDb(l logger.Interface, dbHost string, dbPort int, dbUsername, dbPassword, dbName, sslMode string) (tx *gorm.DB, err error) {
	// Create DB connection
	dataSourceName := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		dbHost, dbPort, dbUsername, dbPassword, dbName, sslMode)
	db, err := gorm.Open(postgres.Open(dataSourceName),
		&gorm.Config{
			Logger: l,
		})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	// Ensure DB connection works
	if err = sqlDB.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
