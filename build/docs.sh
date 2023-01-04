#!/bin/bash
set -eux

gomarkdoc -vv -o docs/DOCUMENTATION.md github.com/vmware-labs/multi-tenant-persistence-for-saas/data-access-layer/datastore --repository.default-branch main --repository.path /
