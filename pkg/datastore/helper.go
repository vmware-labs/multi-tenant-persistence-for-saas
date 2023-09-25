// Copyright 2023 VMware, Inc.
// Licensed to VMware, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. VMware, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
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
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer"
	"github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
)

const (
	// Env. variable names.
	DB_NAME_ENV_VAR           = "DB_NAME"
	DB_PORT_ENV_VAR           = "DB_PORT"
	DB_HOST_ENV_VAR           = "DB_HOST"
	SSL_MODE_ENV_VAR          = "SSL_MODE"
	DB_ADMIN_USERNAME_ENV_VAR = "DB_ADMIN_USERNAME"
	DB_ADMIN_PASSWORD_ENV_VAR = "DB_ADMIN_PASSWORD"

	// SQL Error Codes.
	ERROR_DUPLICATE_KEY      = "SQLSTATE 23505"
	ERROR_DUPLICATE_DATABASE = "SQLSTATE 42P04"
)

type DBConfig struct {
	host     string
	port     int
	username string
	password string
	dbName   string
	sslMode  string
}

// Returns DBConfig constructed from the env variables.
// If dbName is set, it is used instead of DB_NAME env variable.
// All env variables are required and if not set, this method would panic.
func ConfigFromEnv(dbName string) DBConfig {
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
			panic(ErrMissingEnvVar.WithValue(ENV_VAR, envVar))
		}

		if envVarValue := strings.TrimSpace(os.Getenv(envVar)); len(envVarValue) == 0 {
			panic(ErrMissingEnvVar.WithValue(ENV_VAR, envVar).WithValue(VALUE, os.Getenv(envVar)))
		}
	}

	var cfg DBConfig
	var err error

	cfg.host = strings.TrimSpace(os.Getenv(DB_HOST_ENV_VAR))
	// Ensure port number is valid
	if cfg.port, err = strconv.Atoi(strings.TrimSpace(os.Getenv(DB_PORT_ENV_VAR))); err != nil {
		panic(ErrInvalidPortNumber.Wrap(err).WithValue(ENV_VAR, DB_PORT_ENV_VAR).WithValue(VALUE, os.Getenv(DB_PORT_ENV_VAR)))
	}
	cfg.username = strings.TrimSpace(os.Getenv(DB_ADMIN_USERNAME_ENV_VAR))
	cfg.password = strings.TrimSpace(os.Getenv(DB_ADMIN_PASSWORD_ENV_VAR))
	cfg.dbName = strings.ToLower(strings.TrimSpace(os.Getenv(DB_NAME_ENV_VAR)))
	cfg.sslMode = strings.TrimSpace(os.Getenv(SSL_MODE_ENV_VAR))

	if dbName != "" {
		cfg.dbName = strings.ToLower(dbName)
	}
	return cfg
}

func FromEnv(l *logrus.Entry, a authorizer.Authorizer, instancer authorizer.Instancer) (d DataStore, err error) {
	return FromConfig(l, a, instancer, ConfigFromEnv(""))
}

func FromEnvWithDB(l *logrus.Entry, a authorizer.Authorizer, instancer authorizer.Instancer, dbName string) (d DataStore, err error) {
	cfg := ConfigFromEnv(dbName)
	err = DBCreate(cfg)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	return FromConfig(l, a, instancer, cfg)
}

func FromConfig(l *logrus.Entry, a authorizer.Authorizer, instancer authorizer.Instancer, cfg DBConfig) (d DataStore, err error) {
	gl := GetGormLogger(l)
	dbConnInitializer := func(db *relationalDb, dbRole dbrole.DbRole) error {
		db.Lock()
		defer db.Unlock()
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
					err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt).WithValue(DB_NAME, db.dbName)
					// Suppresses following duplicate insertion error,
					// ERROR: duplicate key value violates unique constraint
					// "pg_authid_rolname_index" (SQLSTATE 23505)
					if strings.Contains(tx.Error.Error(), ERROR_DUPLICATE_KEY) {
						db.logger.Debugln(tx.Error)
						return nil
					}
					db.logger.Errorln(err)
					return err
				}
			}
			functionName, functionBody := getCheckAndUpdateRevisionFunc()
			stmt := getCreateTriggerFunctionStmt(functionName, functionBody)
			if tx := db.gormDBMap[dbrole.MAIN].Exec(stmt); tx.Error != nil {
				// Suppresses following duplicate insertion error,
				// ERROR: duplicate key value violates unique constraint
				// "pg_authid_rolname_index" (SQLSTATE 23505)
				if strings.Contains(tx.Error.Error(), ERROR_DUPLICATE_KEY) {
					db.logger.Debugln(tx.Error)
					return nil
				}
				err = ErrExecutingSqlStmt.Wrap(tx.Error).WithValue(SQL_STMT, stmt).WithValue(DB_NAME, db.dbName)
				db.logger.Error(err)
				return err
			}
			return nil
		}

		dbUserSpec := getDbUser(dbRole)
		if _, ok := db.gormDBMap[dbUserSpec.username]; ok {
			return nil
		}
		db.logger.Debugf("Connecting to database %s@%s:%d[%s] ...", dbUserSpec.username, cfg.host, cfg.port, cfg.dbName)
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
		db.logger.Debugf("Connecting to database %s@%s:%d[%s] succeeded",
			dbUserSpec.username, cfg.host, cfg.port, cfg.dbName)

		return nil
	}
	return &relationalDb{
		dbName:      cfg.dbName,
		authorizer:  a,
		instancer:   instancer,
		gormDBMap:   make(map[dbrole.DbRole]*gorm.DB),
		initializer: dbConnInitializer,
		logger:      l,
		txFetcher:   authorizer.SimpleTransactionFetcher{},
	}, nil
}

// Opens a Postgres DB using the provided config. parameters.
func openDb(l logger.Interface, dbHost string, dbPort int, dbUsername, dbPassword, dbName, sslMode string) (tx *gorm.DB, err error) {
	// Create DB connection
	dataSourceName := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		dbHost, dbPort, dbUsername, dbPassword, dbName, sslMode)
	_ = l
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

// Create a Postgres DB using the provided config if it doesn't exist.
func DBCreate(cfg DBConfig) error {
	if DBExists(cfg) {
		return nil
	}
	dataSourceName := fmt.Sprintf("host=%s port=%d user=%s password=%s sslmode=%s",
		cfg.host, cfg.port, cfg.username, cfg.password, cfg.sslMode)
	db, err := gorm.Open(postgres.Open(dataSourceName), &gorm.Config{})
	if err != nil {
		return err
	}

	createDbCmd := fmt.Sprintf("CREATE DATABASE %s;", cfg.dbName)
	if err := db.Exec(createDbCmd).Error; err != nil {
		if strings.Contains(err.Error(), ERROR_DUPLICATE_DATABASE) {
			return nil
		}
		return err
	}
	return nil
}

// Checks if a Postgres DB exists and returns true.
func DBExists(cfg DBConfig) bool {
	dataSourceName := fmt.Sprintf("host=%s port=%d user=%s password=%s sslmode=%s",
		cfg.host, cfg.port, cfg.username, cfg.password, cfg.sslMode)
	db, err := gorm.Open(postgres.Open(dataSourceName), &gorm.Config{})
	if err != nil {
		return false
	}

	selectDb := fmt.Sprintf("SELECT datname FROM pg_database WHERE datname='%s';", cfg.dbName)
	return db.Exec(selectDb).RowsAffected == 1
}
