#!/bin/bash
set -eux

. $(dirname "$0")/env.sh

$(dirname $0)/typos.sh
$(go env GOPATH)/bin/shfmt -d -ci -i 2 .
$(go env GOPATH)/bin/golangci-lint run -v

gomarkdoc -v -c -o docs/DOCUMENTATION.md \
  github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer \
  github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore \
  github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole \
  github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors \
  github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/protostore || (
  $(dirname "$0")/docs.sh
  git diff .
)
