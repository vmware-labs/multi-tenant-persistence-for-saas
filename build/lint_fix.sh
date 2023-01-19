#!/bin/bash
set -eux

. $(dirname "$0")/env.sh

$(dirname $0)/typos.sh --fix
$(go env GOPATH)/bin/golangci-lint run --fix -v
$(go env GOPATH)/bin/golangci-lint run --enable-all -c /dev/null --fix || echo "Ignore failures in DISABLED linters ..."

go vet ./...
go fix ./...
find . | grep -e \.go$ | grep -v vendor\/ | xargs -t -I ARGS sh -c 'goimports -w ARGS; gofmt -w ARGS'

$(go env GOPATH)/bin/shfmt -w -ci -i 2 .
