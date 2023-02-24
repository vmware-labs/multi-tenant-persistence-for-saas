#!/bin/bash
set -eux

PROJECT_ROOT=$PWD

source $(dirname "$0")/setup_local_postgres.sh
source $(dirname "$0")/env.sh

echo "Results Folder " $RESULTS_DIR

# Create the folder if needed (for go coverage tools)
if [ -d $RESULTS_DIR ]; then
  echo "Results folder already exists, cleaning up the directory " $RESULTS_DIR
  rm -rf $RESULTS_DIR/*
fi

echo "Creating results directory to store unit tests result"
mkdir -p $RESULTS_DIR

if [[ "unset" == "${LOG_LEVEL:-unset}" ]]; then
  TEST_VERBOSE=""
else
  TEST_VERBOSE="-v "
fi

echo "running go test, excluding test and proto folders"
CGO_ENABLED=1 go test ${TEST_VERBOSE}$(go list ./... | grep -v '/test\|/api') -coverprofile=${RESULTS_DIR}/coverage.out

retVal=$?

echo "The return value from running tests " $retVal
if [ $retVal -ne 0 ]; then
  echo "Unit tests failed"
  exit $retVal
fi

# Generate total coverage result
go tool cover -func ${RESULTS_DIR}/coverage.out | awk 'END{print $NF}' >${RESULTS_DIR}/totalcoverage.txt
# Generate coverage HTML
go tool cover -html=${RESULTS_DIR}/coverage.out -o=${RESULTS_DIR}/coverage.html

more ${RESULTS_DIR}/*total* | cat
