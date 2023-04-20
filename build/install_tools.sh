#!/bin/bash
set -eux

source $(dirname "$0")/env.sh

GOLNT_VERSION=v1.50.1
GOMRK_VERSION=v0.4.2-0.20230402163522-cc78abbcb570
SHFMT_VERSION=v3.6.0
TYPOS_VERSION=1.13.6

go-licenser -version || go install github.com/elastic/go-licenser@latest
$(go env GOPATH)/bin/shfmt -version | grep ${SHFMT_VERSION} || go install mvdan.cc/sh/v3/cmd/shfmt@${SHFMT_VERSION}
$(go env GOPATH)/bin/gomarkdoc --version | grep ${GOMRK_VERSION} || go install github.com/princjef/gomarkdoc/cmd/gomarkdoc@${GOMRK_VERSION}
$(go env GOPATH)/bin/golangci-lint version | grep ${GOLNT_VERSION} || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin ${GOLNT_VERSION}

if [[ "${OSTYPE}" == "linux"* ]]; then
  /tmp/typos --version | grep ${TYPOS_VERSION} || wget -qO- https://github.com/crate-ci/typos/releases/download/v${TYPOS_VERSION}/typos-v${TYPOS_VERSION}-x86_64-unknown-linux-musl.tar.gz | tar -zxf - -C /tmp/ ./typos
elif [[ "${OSTYPE}" == "darwin"* ]]; then
  /tmp/typos --version | grep ${TYPOS_VERSION} || wget -qO- https://github.com/crate-ci/typos/releases/download/v${TYPOS_VERSION}/typos-v${TYPOS_VERSION}-x86_64-apple-darwin.tar.gz | tar -zxf - -C /tmp/ ./typos
fi

pip3 install addlicense mdv yamllint
sudo snap install mdl
