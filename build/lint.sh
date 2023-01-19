#!/bin/bash
set -eux

. $(dirname "$0")/env.sh

$(dirname $0)/typos.sh
$(go env GOPATH)/bin/shfmt -d -ci -i 2 .
$(go env GOPATH)/bin/golangci-lint run -v

gomarkdoc -c -o docs/DOCUMENTATION.md github.com/vmware-labs/multi-tenant-persistence-for-saas/data-access-layer/datastore --repository.default-branch main --repository.path /
