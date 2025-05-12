#!/bin/bash

# Should be executed by root

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
COLUMNSTORE_SOURCE_PATH=$(realpath "$SCRIPT_LOCATION"/../)

source "$SCRIPT_LOCATION"/utils.sh

echo "Arguments received: $@"

optparse.define short=i long=docker-image desc="Name of the Docker container to execute commands in" variable=DOCKER_IMAGE
optparse.define short=p long=pkg-format desc="Package format used inside the container (rpm or deb)" variable=PKG_FORMAT
optparse.define short=r long=result-path desc="Name suffix used in core dump file path" variable=RESULT
optparse.define short=s long=do-setup desc="Run setup-repo.sh inside the container before installing packages" variable=DO_SETUP
source $(optparse.build)

if [[ "$EUID" -ne 0 ]]; then
    error "Please run script as root"
    exit 1
fi

if [[ -z $(docker ps -q --filter "name=${DOCKER_IMAGE}") ]]; then
  error "Container '${DOCKER_IMAGE}' is not running."
  exit 1
fi

if [[ -z "${DOCKER_IMAGE:-}" || -z "${PKG_FORMAT:-}" || -z "${RESULT:-}" || -z "${DO_SETUP:-}" ]]; then
  echo "Please provide provide --docker-image, --pkg-format, --result-path and --do-setup parameters, e.g. ./prepare_test_stage.sh --docker-image smoke11212, --pkg-format rpm, --result-path /result --do-setup true"
  exit 1
fi

execInnerDocker() {  #TODO: move to utils.sh?
    local cmd="$1"
    local img="$2"
    local flags="${3:-}"
    docker exec $flags -t "$img" sh -c "$cmd"
}

apk add bash && bash "$COLUMNSTORE_SOURCE_PATH"/core_dumps/docker-awaiter.sh "$DOCKER_IMAGE"

if [[ "$PKG_FORMAT" = "deb" ]]; then
    execInnerDocker 'sed -i "s/exit 101/exit 0/g" /usr/sbin/policy-rc.d' "$DOCKER_IMAGE"
fi

echo "Docker CGroups opts here"
ls -al /sys/fs/cgroup/cgroup.controllers || true
ls -al /sys/fs/cgroup/       || true
ls -al /sys/fs/cgroup/memory || true

docker ps --filter name="$DOCKER_IMAGE"

execInnerDocker 'echo "Inner Docker CGroups opts here"' "$DOCKER_IMAGE"
execInnerDocker 'ls -al /sys/fs/cgroup/cgroup.controllers || true' "$DOCKER_IMAGE"
execInnerDocker 'ls -al /sys/fs/cgroup/       || true' "$DOCKER_IMAGE"
execInnerDocker 'ls -al /sys/fs/cgroup/memory || true' "$DOCKER_IMAGE"

#Prepare core dump directory inside container
execInnerDocker 'mkdir -p /core' "$DOCKER_IMAGE"
execInnerDocker 'chmod 777 /core' "$DOCKER_IMAGE"


docker cp "$COLUMNSTORE_SOURCE_PATH"/core_dumps/.      "$DOCKER_IMAGE":/      #TODO: make all path right
docker cp "$COLUMNSTORE_SOURCE_PATH"/build/utils.sh    "$DOCKER_IMAGE":/
docker cp "$COLUMNSTORE_SOURCE_PATH"/setup-repo.sh     "$DOCKER_IMAGE":/


if [[ "$DO_SETUP" == "true" ]]; then
    execInnerDocker "/setup-repo.sh" "$DOCKER_IMAGE"
fi

#Install deps
if [[ "$PKG_FORMAT" = "rpm" ]]; then
    execInnerDocker 'bash -c "yum install -y cracklib-dicts diffutils elfutils epel-release findutils iproute gawk gcc-c++ gdb hostname lz4 patch perl procps-ng rsyslog sudo tar wget which"' "$DOCKER_IMAGE"
else
    execInnerDocker 'bash -c "apt update --yes && apt install -y elfutils findutils iproute2 g++ gawk gdb hostname liblz4-tool patch procps rsyslog sudo tar wget"' "$DOCKER_IMAGE"
fi

# Configure core dump naming pattern
execInnerDocker "sysctl -w kernel.core_pattern=\"/core/%E_${RESULT}_core_dump.%p\"" "$DOCKER_IMAGE"

echo "PrepareTestStage completed in $DOCKER_IMAGE"