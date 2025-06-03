#!/bin/bash

# Should be executed by root
set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
COLUMNSTORE_SOURCE_PATH=$(realpath "$SCRIPT_LOCATION"/../)

source "$SCRIPT_LOCATION"/utils.sh

echo "Arguments received: $@"

optparse.define short=c long=container-name desc="Name of the Docker container to run tests in" variable=CONTAINER_NAME
optparse.define short=i long=docker-image      desc="Docker image name to start container from" variable=DOCKER_IMAGE
optparse.define short=r long=result-path       desc="Name suffix used in core dump file path"   variable=RESULT
optparse.define short=s long=do-setup          desc="Run setup-repo.sh inside the container"    variable=DO_SETUP
optparse.define short=u long=packages-url      desc="Packages url"                              variable=PACKAGES_URL
source $(optparse.build)

if [[ "$EUID" -ne 0 ]]; then
    error "Please run script as root"
    exit 1
fi

if [[ -z "${CONTAINER_NAME:-}" || -z "${DOCKER_IMAGE:-}" || -z "${RESULT:-}" || -z "${DO_SETUP:-}" || -z "${PACKAGES_URL:-}" ]]; then
  echo "Please provide --container-name, --docker-image, --result-path, --packages-url and --do-setup parameters, e.g. ./prepare_test_stage.sh --container-name smoke11212 --docker-image detravi/ubuntu:24.04 --result-path ubuntu24.04 --packages-url https://cspkg.s3.amazonaws.com/stable-23.10/pull_request/91/10.6-enterprise --do-setup true"
  exit 1
fi

start_container() {
    if [[ "$RESULT" == *rocky* ]]; then
        SYSTEMD_PATH="/usr/lib/systemd/systemd"
        MTR_PATH="/usr/share/mysql-test"
    else
        SYSTEMD_PATH="systemd"
        MTR_PATH="/usr/share/mysql/mysql-test"
    fi

    docker_run_args=(
        --env OS="$RESULT"
        --env PACKAGES_URL="$PACKAGES_URL"
        --env DEBIAN_FRONTEND=noninteractive
        --env MCS_USE_S3_STORAGE=0
        --name "$CONTAINER_NAME"
        --ulimit core=-1
        --privileged
        --detach
    )

    if [[ "$CONTAINER_NAME" == *smoke* ]]; then
        docker_run_args+=( --memory 3g )
    elif [[ "$CONTAINER_NAME" == *mtr* ]]; then
        docker_run_args+=( --shm-size=500m --memory 8g --env MYSQL_TEST_DIR="$MTR_PATH" )
    elif [[ "$CONTAINER_NAME" == *cmapi* ]]; then
        docker_run_args+=( --env PYTHONPATH="${PYTHONPATH}" )
    elif [[ "$CONTAINER_NAME" == *upgrade* ]]; then
        docker_run_args+=( --env UCF_FORCE_CONFNEW=1 --volume /sys/fs/cgroup:/sys/fs/cgroup:ro )
    elif [[ "$CONTAINER_NAME" == *regression* ]]; then
        docker_run_args+=( --shm-size=500m --memory 12g )
    else
        echo "Unknown container type: $CONTAINER_NAME"
        exit 1
    fi

    docker_run_args+=( "$DOCKER_IMAGE" "$SYSTEMD_PATH" --unit=basic.target )

    docker run "${docker_run_args[@]}"
    sleep 5

    apk add bash
    bash "$COLUMNSTORE_SOURCE_PATH"/core_dumps/docker-awaiter.sh "$CONTAINER_NAME"

    if ! docker ps -q --filter "name=${CONTAINER_NAME}" | grep -q .; then
        error "Container '$CONTAINER_NAME' has not started!"
        exit 1
    fi
}

start_container

if [[ "$RESULT" != *rocky* ]]; then
    execInnerDocker 'sed -i "s/exit 101/exit 0/g" /usr/sbin/policy-rc.d' "$CONTAINER_NAME"
fi

#list_cgroups
echo "Docker CGroups opts here"
ls -al /sys/fs/cgroup/cgroup.controllers || true
ls -al /sys/fs/cgroup/       || true
ls -al /sys/fs/cgroup/memory || true

execInnerDocker 'echo Inner Docker CGroups opts here'                            "$CONTAINER_NAME"
execInnerDocker 'ls -al /sys/fs/cgroup/cgroup.controllers || true'               "$CONTAINER_NAME"
execInnerDocker 'ls -al /sys/fs/cgroup/ || true'                                 "$CONTAINER_NAME"
execInnerDocker 'ls -al /sys/fs/cgroup/memory || true'                           "$CONTAINER_NAME"

# Prepare core dump directory inside container
execInnerDocker 'mkdir -p core && chmod 777 core'           "$CONTAINER_NAME"
docker cp "$COLUMNSTORE_SOURCE_PATH"/core_dumps/.           "$CONTAINER_NAME":/
docker cp "$COLUMNSTORE_SOURCE_PATH"/build/utils.sh         "$CONTAINER_NAME":/
docker cp "$COLUMNSTORE_SOURCE_PATH"/setup-repo.sh          "$CONTAINER_NAME":/

if [[ "$DO_SETUP" == "true" ]]; then
    execInnerDocker '/setup-repo.sh'                   "$CONTAINER_NAME"
fi

# install deps
if [[ "$RESULT" == *rocky* ]]; then
    execInnerDocker 'yum update -y && yum install -y cracklib-dicts diffutils elfutils epel-release findutils iproute gawk gcc-c++ gdb hostname lz4 patch perl procps-ng rsyslog sudo tar wget which'                   "$CONTAINER_NAME"
    # sudo bypass
    execInnerDocker "printf '%s\n' '#!/bin/sh' 'exec \"\$@\"' > /usr/bin/sudo && chmod +x /usr/bin/sudo" "$CONTAINER_NAME"
else
    execInnerDocker 'apt update -y && apt install -y elfutils findutils iproute2 g++ gawk gdb hostname liblz4-tool patch procps rsyslog sudo tar wget' "$CONTAINER_NAME"
fi

# Configure core dump naming pattern
execInnerDocker 'sysctl -w kernel.core_pattern="/core/%E_${RESULT}_core_dump.%p"' "$CONTAINER_NAME"

#Install columnstore in container
echo "Installing columnstore..."
if [[ "$RESULT" == *rocky* ]]; then
    execInnerDocker 'yum install -y MariaDB-columnstore-engine MariaDB-test' "$CONTAINER_NAME"
else
    execInnerDocker 'apt update -y && apt install -y mariadb-plugin-columnstore mariadb-test' "$CONTAINER_NAME"
fi

sleep 5
echo "PrepareTestStage completed in $CONTAINER_NAME"
