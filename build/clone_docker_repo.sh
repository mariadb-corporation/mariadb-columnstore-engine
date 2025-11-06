#!/bin/bash

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh


# Determine which branch to use
DOCKER_REF="${DOCKER_REF:-}"
DOCKER_BRANCH_REF="${DRONE_SOURCE_BRANCH:-stable-23.10}"
DOCKER_REF_AUX="stable-23.10"

message "Checking for docker repository branch: $DOCKER_BRANCH_REF"

# Try to find matching branch in docker repository
if [[ -z "$DOCKER_REF" ]]; then
    DOCKER_REF=$(git ls-remote https://github.com/mariadb-corporation/mariadb-columnstore-docker --heads --sort origin "refs/heads/$DOCKER_BRANCH_REF" 2>/dev/null | grep -E -o "[^/]+$" || true)
fi

# Fall back to default branch if no match found
if [[ -z "$DOCKER_REF" ]]; then
    DOCKER_REF="$DOCKER_REF_AUX"
fi

message "Cloning docker repository from branch: $DOCKER_REF"

# Remove existing docker folder if it exists to avoid git clone conflicts
if [[ -d "docker" ]]; then
    message "Removing existing docker folder..."
    rm -rf docker
fi

# Clone the docker repository
git clone --branch "$DOCKER_REF" --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-docker docker

# Create empty .secrets file
touch docker/.secrets

message "Docker repository cloned successfully"
