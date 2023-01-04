#!/bin/bash
set -eux

source $(dirname "$0")/setup_local_postgres.sh

go test ./... -run=^$ -bench=Benchmark -count=1 --benchtime=3s -benchmem
