#!/bin/bash
set -eux
. $(dirname "$0")/env.sh

# Not fully tested will need handholding on failures
go clean -modcache
go get -u ./...
go mod tidy --compat=1.18
