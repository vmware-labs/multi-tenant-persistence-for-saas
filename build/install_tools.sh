#!/bin/bash
set -eux

source $(dirname "$0")/env.sh

FORCE=false
GODLV_VERSION=${GODLV_VERSION:=1.21.0}
if go version | grep 1.2; then
  GOLNT_VERSION=${GOLNT_VERSION:=1.52.2}
else
  GOLNT_VERSION=${GOLNT_VERSION:=1.50.1}
fi
GOMRK_VERSION=${GOMRK_VERSION:=1.1.0}
SHFMT_VERSION=${SHFMT_VERSION:=3.6.0}
GOIMP_VERSION=${GOIMP_VERSION:=0.5.0}
GOIMPORTS_REVISER_VERSION=${GOIMPORTS_REVISER_VERSION:=3.4.2}
TYPOS_VERSION=${TYPOS_VERSION:=1.13.6}
ADDLICENSE_VERSION=${ADDLICENSE_VERSION:=1.1.1}
CODEOWNERS_VALIDATOR_VERSION=${CODEOWNERS_VALIDATOR_VERSION:=0.7.4}
YAMLFMT_VERSION=${YAMLFMT_VERSION:=0.9.0}

# With version check
go_install() {
  ([ ${FORCE} = false ] && $1 | grep $2) || go install $3@v$2
}

# Without version check
go_install_any() {
  ([ ${FORCE} = false ] && $1 >/dev/null 2>&1) || go install $2@$3
}

go version | grep '1.19\|1.20\|1.21' || (
  echo "Install supported version (>=1.19) of golang to use saas-ci"
  exit 1
)
go_install "dlv version" ${GODLV_VERSION} github.com/go-delve/delve/cmd/dlv
go_install "golangci-lint version" ${GOLNT_VERSION} github.com/golangci/golangci-lint/cmd/golangci-lint
go_install "gomarkdoc --version" ${GOMRK_VERSION} github.com/princjef/gomarkdoc/cmd/gomarkdoc
go_install "shfmt -version" ${SHFMT_VERSION} mvdan.cc/sh/v3/cmd/shfmt
go_install_any "goimports -h" golang.org/x/tools/cmd/goimports v${GOIMP_VERSION}
go_install_any "goimports-reviser -h" "github.com/incu6us/goimports-reviser/v3" v${GOIMPORTS_REVISER_VERSION}
go_install_any "golicenser -version" github.com/elastic/go-licenser latest
go_install_any "addlicense -h" github.com/google/addlicense v${ADDLICENSE_VERSION}
go_install_any "codeowners-validator -v" github.com/mszostok/codeowners-validator v${CODEOWNERS_VALIDATOR_VERSION}
go_install_any "yamlfmt -h" github.com/google/yamlfmt/cmd/yamlfmt v${YAMLFMT_VERSION}

OS_TYPE=$(uname -s)
if [ "${OS_TYPE}" = "Linux" ]; then
  /tmp/typos --version | grep ${TYPOS_VERSION} || wget -qO- https://github.com/crate-ci/typos/releases/download/v${TYPOS_VERSION}/typos-v${TYPOS_VERSION}-x86_64-unknown-linux-musl.tar.gz | tar -zxf - -C /tmp/ ./typos
  sudo snap install mdl
elif [ "${OS_TYPE}" = "Darwin" ]; then
  /tmp/typos --version | grep ${TYPOS_VERSION} || wget -qO- https://github.com/crate-ci/typos/releases/download/v${TYPOS_VERSION}/typos-v${TYPOS_VERSION}-x86_64-apple-darwin.tar.gz | tar -zxf - -C /tmp/ ./typos
fi
pip3 install addlicense mdv yamllint
