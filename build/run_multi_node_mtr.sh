#!/bin/bash

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

#usage example: sudo ./run_multi_node_mtr.sh  --columnstore-image-name mariadb/enterprise-columnstore-dev:stable-23.10-pull-request2240  --distro rockylinux:8
optparse.define short=i long=columnstore-image-name desc="Name of columnstore docker image" variable=MCS_IMAGE_NAME
optparse.define short=d long=distro desc="Linux distro for which multinode mtr is executed" variable=DISTRO
source $(optparse.build)

echo "Arguments received: $@"

if [[ "$EUID" -ne 0 ]]; then
    error "Please run script as root"
    exit 1
fi

for flag in MCS_IMAGE_NAME DISTRO; do
  if [[ -z "${!flag}" ]]; then
    error "Missing required flag: -${flag:0:1} / --${flag,,}"
    exit 1
  fi
done

select_pkg_format ${DISTRO}

if [[ "$PKG_FORMAT" == "rpm" ]]; then
    SOCKET_PATH="/var/lib/mysql/mysql.sock"
    MTR_PATH="/usr/share/mysql-test"
else
    SOCKET_PATH="/run/mysqld/mysqld.sock"
    MTR_PATH="/usr/share/mysql/mysql-test"
fi

message "Running multinode mtr tests..."

# Detect docker compose command (new syntax vs old)
if command -v docker &> /dev/null && docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
elif command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
else
    error "Docker compose is not installed!"
    exit 1
fi

# Handle Docker Hub authentication
message "Checking Docker Hub authentication..."
if [[ -n "${DOCKER_LOGIN:-}" ]] && [[ -n "${DOCKER_PASSWORD:-}" ]]; then
    # CI environment - use credentials from secrets
    message "Using Docker credentials from environment variables"
    echo "$DOCKER_PASSWORD" | docker login --username "$DOCKER_LOGIN" --password-stdin
    if [[ $? -ne 0 ]]; then
        error "Docker login failed with provided credentials"
        exit 1
    fi
else
    message "No Docker credentials in env variables found, checking if already logged in..."
    
    # Try to verify docker login by checking auth for docker.io
    if docker system info 2>&1 | grep -q "Username:"; then
        message "Already logged in to Docker Hub"
    else
        message "Not logged in to Docker Hub"
        message "You need to login to Docker Hub to pull enterprise-columnstore-dev images"
        message ""
        
        # Prompt for username
        read -p "Docker Hub Username: " docker_username
        
        # Run docker login with username (will prompt for password)
        docker login -u "$docker_username"
        
        if [[ $? -ne 0 ]]; then
            error "Docker login failed or was cancelled"
            exit 1
        fi
        
        message "Docker login successful"
    fi
fi

# Check if docker folder exists and has required files
if [[ ! -d "docker" ]] || [[ ! -f "docker/Dockerfile" ]]; then
    message "Docker folder not found or incomplete, cloning mariadb-columnstore-docker repository..."
    "$SCRIPT_LOCATION"/clone_docker_repo.sh
fi

cd docker

# Clean up any existing containers from previous runs
if docker ps -a --format '{{.Names}}' | grep -q -E '^(mcs1|mcs2|mcs3)$'; then
    message "Found existing containers from previous run, cleaning up..."
    $DOCKER_COMPOSE down -v 2>/dev/null || true
    docker rm -f mcs1 mcs2 mcs3 2>/dev/null || true
    message "Cleanup completed"
fi

cp .env_example .env
sed -i "/^MCS_IMAGE_NAME=/s|=.*|=${MCS_IMAGE_NAME}|" .env
sed -i "/^MAXSCALE=/s|=.*|=false|" .env

message "Starting containers..."
$DOCKER_COMPOSE up -d

message "Provisioning cluster..."
docker exec mcs1 provision mcs1 mcs2 mcs3
docker cp ../mysql-test/columnstore mcs1:"${MTR_PATH}/suite/"
docker exec -t mcs1 chown -R mysql:mysql "${MTR_PATH}"
docker exec -t mcs1 mariadb -e "CREATE DATABASE IF NOT EXISTS test;"

docker exec -t mcs1 bash -c "\
  cd '${MTR_PATH}' && \
  ./mtr \
    --extern socket='${SOCKET_PATH}' \
    --force \
    --print-core=detailed \
    --print-method=gdb \
    --max-test-fail=0 \
    --suite=columnstore/basic,columnstore/bugfixes \
"
