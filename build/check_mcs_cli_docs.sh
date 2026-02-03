#!/bin/bash
# Script to verify MCS CLI documentation is up-to-date
# This script generates fresh docs (README.md and mcs.1 man page) from the
# installed cmapi and compares them to the committed versions in the source.
# Fails if there are differences.
#
# CI mode: check_mcs_cli_docs.sh --container-name <name>
#   Runs inside a Docker container with cmapi installed
#
# Local mode: check_mcs_cli_docs.sh [--update]
#   Runs locally using system Python or virtual environment
#   --update: Update the committed docs instead of just checking

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
COLUMNSTORE_SOURCE_PATH=$(realpath "${SCRIPT_LOCATION}/../")
# Installed cmapi path (inside container)
CMAPI_INSTALLED_PATH="/usr/share/columnstore/cmapi"
# Source path for committed docs
CMAPI_SOURCE_PATH="${COLUMNSTORE_SOURCE_PATH}/cmapi"
MCS_TOOL_SOURCE_DIR="${CMAPI_SOURCE_PATH}/mcs_cluster_tool"

source "$SCRIPT_LOCATION"/utils.sh

optparse.define short=c long=container-name desc="Name of the Docker container to run docs check in" variable=CONTAINER_NAME
optparse.define short=u long=update desc="Update the committed docs instead of just checking" variable=UPDATE_MODE default=false value=true
source $(optparse.build)
message "Arguments received: $@"

# CI mode: run inside container
if [[ -n "${CONTAINER_NAME}" ]]; then
    echo "=== Checking MCS CLI documentation freshness (CI mode) ==="
    echo "Container: ${CONTAINER_NAME}"

    # Install cmapi package in container
    echo "Installing cmapi package..."
    execInnerDockerWithRetry "$CONTAINER_NAME" "yum install -y MariaDB-columnstore-cmapi" || \
        execInnerDockerWithRetry "$CONTAINER_NAME" "apt-get update && apt-get install -y mariadb-columnstore-cmapi"

    # Install md2man for man page generation
    echo "Installing md2man..."
    execInnerDockerWithRetry "$CONTAINER_NAME" "yum install -y ruby ruby-devel make gcc redhat-rpm-config && gem install md2man --no-document" || \
        execInnerDockerWithRetry "$CONTAINER_NAME" "apt-get install -y ruby ruby-dev make gcc && gem install md2man --no-document"

    # Generate fresh README.md inside container
    echo "Generating fresh README.md from installed cmapi..."
    execInnerDockerWithRetry "$CONTAINER_NAME" "cd ${CMAPI_INSTALLED_PATH} && PYTHONPATH=${CMAPI_INSTALLED_PATH}:${CMAPI_INSTALLED_PATH}/deps python/bin/python3 -m typer mcs_cluster_tool/__main__.py utils docs --name mcs --output /tmp/README.md"

    # Generate fresh mcs.1 man page
    echo "Generating fresh mcs.1 man page..."
    execInnerDockerWithRetry "$CONTAINER_NAME" "md2man-roff /tmp/README.md > /tmp/mcs.1"

    # Copy generated files out of container for comparison
    TEMP_DIR=$(mktemp -d)
    trap "rm -rf -- '${TEMP_DIR}'" EXIT

    docker cp "${CONTAINER_NAME}:/tmp/README.md" "${TEMP_DIR}/README.md"
    docker cp "${CONTAINER_NAME}:/tmp/mcs.1" "${TEMP_DIR}/mcs.1"

    # Compare with committed versions
    DOCS_OUTDATED=false

    echo "Comparing generated README.md with committed version..."
    if ! diff -q "${MCS_TOOL_SOURCE_DIR}/README.md" "${TEMP_DIR}/README.md" >/dev/null 2>&1; then
        DOCS_OUTDATED=true
        echo ""
        echo "=========================================="
        echo "ERROR: README.md is outdated!"
        echo "=========================================="
        echo ""
        echo "Diff (committed vs generated) (first 100 lines):"
        diff -u "${MCS_TOOL_SOURCE_DIR}/README.md" "${TEMP_DIR}/README.md" | head -100 || true
    fi

    echo "Comparing generated mcs.1 with committed version..."
    if ! diff -q "${MCS_TOOL_SOURCE_DIR}/mcs.1" "${TEMP_DIR}/mcs.1" >/dev/null 2>&1; then
        DOCS_OUTDATED=true
        echo ""
        echo "=========================================="
        echo "ERROR: mcs.1 man page is outdated!"
        echo "=========================================="
        echo ""
        echo "Diff (committed vs generated):"
        diff -u "${MCS_TOOL_SOURCE_DIR}/mcs.1" "${TEMP_DIR}/mcs.1" | head -100 || true
    fi

    if [ "${DOCS_OUTDATED}" = true ]; then
        echo ""
        echo "To fix this, run locally:"
        echo "  ./build/check_mcs_cli_docs.sh --update"
        echo ""
        echo "Then commit the updated files."
        exit 1
    fi

    echo ""
    echo "MCS CLI documentation is up-to-date (README.md and mcs.1)!"
    exit 0
fi

# Local mode: run on host
echo "=== Checking MCS CLI documentation freshness (local mode) ==="

TEMP_DIR=$(mktemp -d)
trap "rm -rf -- '${TEMP_DIR}'" EXIT

# Determine which Python to use
if [ -x "${CMAPI_SOURCE_PATH}/python/bin/python3" ]; then
    PYTHON="${CMAPI_SOURCE_PATH}/python/bin/python3"
    export PYTHONPATH="${CMAPI_SOURCE_PATH}:${CMAPI_SOURCE_PATH}/deps"
    echo "Using cmapi bundled Python: ${PYTHON}"
else
    PYTHON="python3"
    export PYTHONPATH="${CMAPI_SOURCE_PATH}"
    echo "Using system Python: ${PYTHON}"
    # Install typer if not available
    if ! "${PYTHON}" -c "import typer" 2>/dev/null; then
        echo "Installing typer..."
        if ! ("${PYTHON}" -m pip install typer --quiet || "${PYTHON}" -m pip install typer --quiet --break-system-packages); then
            echo "ERROR: Failed to install typer. Please install it manually ('pip install typer') and re-run the script." >&2
            exit 1
        fi
    fi
fi

# Generate fresh README.md
echo "Generating fresh README.md from current codebase..."
cd "${CMAPI_SOURCE_PATH}"
"${PYTHON}" -m typer mcs_cluster_tool/__main__.py utils docs --name mcs --output "${TEMP_DIR}/README.md"

# Generate fresh mcs.1 man page
if command -v md2man-roff &>/dev/null; then
    echo "Generating fresh mcs.1 man page..."
    md2man-roff "${TEMP_DIR}/README.md" > "${TEMP_DIR}/mcs.1"
else
    echo "WARNING: md2man-roff not available, skipping man page check"
    echo "Install with: gem install md2man"
fi

# Compare with committed versions
DOCS_OUTDATED=false
README_OUTDATED=false
MANPAGE_OUTDATED=false

echo "Comparing generated README.md with committed version..."
if ! diff -q "${MCS_TOOL_SOURCE_DIR}/README.md" "${TEMP_DIR}/README.md" >/dev/null 2>&1; then
    README_OUTDATED=true
    DOCS_OUTDATED=true
    echo ""
    echo "=========================================="
    echo "ERROR: README.md is outdated!"
    echo "=========================================="
    echo ""
    echo "Diff (committed vs generated):"
    diff -u "${MCS_TOOL_SOURCE_DIR}/README.md" "${TEMP_DIR}/README.md" | head -100 || true
fi

if [ -f "${TEMP_DIR}/mcs.1" ]; then
    echo "Comparing generated mcs.1 with committed version..."
    if [ ! -f "${MCS_TOOL_SOURCE_DIR}/mcs.1" ]; then
        MANPAGE_OUTDATED=true
        DOCS_OUTDATED=true
        echo "ERROR: Committed mcs.1 not found"
    elif ! diff -q "${MCS_TOOL_SOURCE_DIR}/mcs.1" "${TEMP_DIR}/mcs.1" >/dev/null 2>&1; then
        MANPAGE_OUTDATED=true
        DOCS_OUTDATED=true
        echo ""
        echo "=========================================="
        echo "ERROR: mcs.1 man page is outdated!"
        echo "=========================================="
        echo ""
        echo "Diff (committed vs generated):"
        diff -u "${MCS_TOOL_SOURCE_DIR}/mcs.1" "${TEMP_DIR}/mcs.1" | head -100 || true
    fi
fi

# Handle update mode or show fix instructions
if [ "${DOCS_OUTDATED}" = true ]; then
    if [ "${UPDATE_MODE}" = true ]; then
        echo ""
        echo "Updating committed documentation..."
        if [ "${README_OUTDATED}" = true ]; then
            cp "${TEMP_DIR}/README.md" "${MCS_TOOL_SOURCE_DIR}/README.md"
            echo "README.md has been updated!"
        fi
        if [ "${MANPAGE_OUTDATED}" = true ] && [ -f "${TEMP_DIR}/mcs.1" ]; then
            cp "${TEMP_DIR}/mcs.1" "${MCS_TOOL_SOURCE_DIR}/mcs.1"
            echo "mcs.1 has been updated!"
        fi
        echo ""
        echo "Do not forget to commit the updated files!"
        exit 0
    fi

    echo ""
    echo "To fix this, run:"
    echo "  ./build/check_mcs_cli_docs.sh --update"
    echo ""
    echo "Then commit the updated files."
    exit 1
fi

echo ""
echo "MCS CLI documentation is up-to-date (README.md and mcs.1)!"
exit 0
