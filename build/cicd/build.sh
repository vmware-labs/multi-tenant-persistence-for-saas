#!/bin/bash
set -eux

export GOPATH=/go
export GO111MODULE=on

build/env.sh
build/lint.sh
build/build.sh
build/unit_tests.sh
build/coverage.sh
