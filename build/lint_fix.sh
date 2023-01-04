#!/bin/bash
set -eux

. $(dirname "$0")/env.sh

$(dirname $0)/typos.sh --fix

go vet ./...
go fix ./...
find . | grep -e \.go$ | grep -v vendor\/ | xargs -t -I ARGS sh -c 'goimports -w ARGS; gofmt -w ARGS'

$(go env GOPATH)/bin/shfmt -w -ci -i 2 .
