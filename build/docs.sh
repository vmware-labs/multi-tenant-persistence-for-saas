#!/bin/bash
set -eux

gomarkdoc -vv -o docs/DOCUMENTATION.md \
  github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/authorizer \
  github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/datastore \
  github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/dbrole \
  github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors \
  github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/protostore
