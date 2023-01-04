#!/bin/bash
set -eux

export GOARCH=amd64 CGO_ENABLED=1
GOOS=
if [[ "${OSTYPE}" == "linux"* ]]; then
  export GOOS=linux
elif [[ "${OSTYPE}" == "darwin"* ]]; then
  export GOOS=darwin
fi
echo "Compiling for GOOS=$GOOS & GOARCH=$GOARCH"
source $(dirname "$0")/build_info.sh

PROJECT_ROOT=$PWD
export RESULTS_DIR=$PROJECT_ROOT/test-results
export SCRATCH_DIRECTORY=$RESULTS_DIR

mkdir -p $SCRATCH_DIRECTORY
