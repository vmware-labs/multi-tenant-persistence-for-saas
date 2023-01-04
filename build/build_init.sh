#!/bin/bash
set -eux

source $(dirname "$0")/env.sh

if [[ -f go.mod ]] && [[ ! -s go.mod ]]; then
  rm go.mod go.sum
  go mod init multi-tenant-persistence-for-saas
  go mod tidy
elif [[ ! -f go.mod ]]; then
  go mod init multi-tenant-persistence-for-saas
  go mod tidy
fi
go mod tidy
