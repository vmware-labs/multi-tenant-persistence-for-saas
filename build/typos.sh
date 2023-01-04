#!/bin/bash
set -eux

if [[ ${1:-nofix} == "--fix" ]]; then
  /tmp/typos -w --format long -c $(dirname $0)/_typos.toml $(git ls-files)
else
  /tmp/typos --format long -c $(dirname $0)/_typos.toml $(git ls-files)
fi
