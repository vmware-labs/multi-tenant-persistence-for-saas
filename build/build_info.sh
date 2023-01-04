#!/bin/bash
set -eux

LD_FLAGS_VALUES=" -X 'main.BuildInfoGitCommit=$(git rev-list -1 HEAD || echo no-git-repo)'"
LD_FLAGS_VALUES+=" -X 'main.BuildInfoMachine=$(hostname)'"
LD_FLAGS_VALUES+=" -X 'main.BuildInfoTimeLocal=$(date)'"
LD_FLAGS_VALUES+=" -X 'main.BuildInfoTimeUtc=$(date -u)'"
LD_FLAGS_VALUES+=" -X 'main.BuildInfoUser=$(id -u -n)'"
LD_FLAGS_VALUES+=" -X 'main.BuildInfoWs=$(pwd)'"
LD_FLAGS_VALUES+=" -X 'main.BuildInfoTgtHw=$GOARCH'"
LD_FLAGS_VALUES+=" -X 'main.BuildInfoTgtOs=$GOOS'"

export LD_FLAGS_VALUES
