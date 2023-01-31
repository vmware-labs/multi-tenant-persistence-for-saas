#!/bin/bash
set -eux
TYPOS=_typos.toml
if [[ ! -f ${TYPOS} ]]; then
  git ls-files || (echo "Unable to find git tree, skipping typos lint checks ..." && exit)
  TYPOS=$(git ls-files $(git rev-parse --show-toplevel) | grep _typos.toml)
  if [[ ! -f ${TYPOS} ]]; then
    echo "Unable to find the config file _typos.toml, skipping typos lint checks ..." && exit
  fi
fi

if [[ ${1:-nofix} == "--fix" ]]; then
  /tmp/typos -w --format long -c ${TYPOS} .
else
  /tmp/typos -v --format long -c ${TYPOS} .
fi
