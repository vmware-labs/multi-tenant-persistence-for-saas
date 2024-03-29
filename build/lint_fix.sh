#!/bin/bash
set -eux

. $(dirname "$0")/env.sh

$(dirname $0)/typos.sh --fix
$(go env GOPATH)/bin/golangci-lint run --fix -v
$(go env GOPATH)/bin/golangci-lint run --enable-all -c /dev/null --fix || echo "Ignore failures in DISABLED linters ..."

go vet ./...
go fix ./...
find . -name "*.go" -print0 | xargs -0 -I {} goimports-reviser --imports-order "std,general,company,project" {}

$(go env GOPATH)/bin/shfmt -w -ci -i 2 .
