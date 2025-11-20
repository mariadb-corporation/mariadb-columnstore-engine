#!/bin/bash
# Run ColumnStore regression tests locally on pre-built packages from S3
#
# Usage:
#   # Minimal (ubuntu:24.04, test000.sh)
#   ./run_regression_locally.sh \
#     -u https://cspkg.s3.amazonaws.com/stable-23.10/pull_request/2463/10.6-enterprise \
#     -t ghp_xxxxxxxxxxxxxxxxxxxx
#
#   ./run_regression_locally.sh \
#     -u https://cspkg.s3.amazonaws.com/stable-23.10/cron/2506/10.6-enterprise \
#     -t ghp_xxxxxxxxxxxxxxxxxxxx \
#     -n all \
#     -p ubuntu:24.04 \
#     -b stable-23.10
#
# Output:
#   ./regression-results/ - Test logs and memory monitor log

set -e

SCRIPT_LOCATION=$(dirname "$0")
source "$SCRIPT_LOCATION"/utils.sh

echo "Arguments received: $@"

optparse.define short=u long=packages-url      desc="S3 URL with pre-built packages"         variable=PACKAGES_URL
optparse.define short=t long=github-token      desc="GitHub token to pull from regression tests repo"         variable=GITHUB_TOKEN
optparse.define short=p long=platform          desc="Docker platform"                         variable=PLATFORM          default="ubuntu:24.04"
optparse.define short=b long=regression-branch desc="Regression repo branch"                  variable=REGRESSION_BRANCH default="stable-23.10"
optparse.define short=n long=test-name         desc="Test name to execute (or 'all')"        variable=TEST_NAME         default="test000.sh"
source $(optparse.build)

# Internal variables
CONTAINER_NAME="regression_local_$$"
REGRESSION_TIMEOUT="2h"
RESULT_PATH="${PLATFORM//:/}"  # ubuntu:24.04 → ubuntu24.04 (no underscore)

# Cleanup on exit
cleanup() {
    [[ -n "${CONTAINER_NAME:-}" ]] && docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Apply image naming logic from jsonnet: "detravi/" + std.strReplace(platform, "/", "-")
DOCKER_IMAGE="detravi/${PLATFORM}"

# Checks
[[ "$EUID" -ne 0 ]] && echo "Run with sudo" && exit 1
! command -v docker &>/dev/null && echo "Docker not installed" && exit 1

# Check required parameters
for flag in PACKAGES_URL GITHUB_TOKEN; do
  if [[ -z "${!flag}" ]]; then
    error "Missing required flag: --${flag,,}"
    [[ "$flag" == "GITHUB_TOKEN" ]] && echo "Create token at: https://github.com/settings/tokens (needs 'repo' scope)"
    exit 1
  fi
done

export GITHUB_TOKEN

echo "Platform: $PLATFORM → Docker image: $DOCKER_IMAGE"
echo "Packages: $PACKAGES_URL"

# Prepare container
echo "→ Preparing container..."
bash "$SCRIPT_LOCATION/prepare_test_container.sh" \
    --container-name "$CONTAINER_NAME" \
    --docker-image "$DOCKER_IMAGE" \
    --result-path "$RESULT_PATH" \
    --packages-url "$PACKAGES_URL" \
    --do-setup true || exit 1

# Run tests
if [[ "$TEST_NAME" == "all" ]]; then
    TESTS=("test000.sh" "test001.sh")
else
    TESTS=("$TEST_NAME")
fi

for test in "${TESTS[@]}"; do
    echo "→ Running test: $test"
    bash "$SCRIPT_LOCATION/run_regression.sh" \
        --container-name "$CONTAINER_NAME" \
        --test-name "$test" \
        --distro "$PLATFORM" \
        --regression-branch "$REGRESSION_BRANCH" \
        --regression-timeout "$REGRESSION_TIMEOUT" || true
done

# Copy test results from container to host
RESULTS_DIR="./regression-results"
rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"
docker cp "$CONTAINER_NAME":/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/. "$RESULTS_DIR/" 2>/dev/null || true

echo ""
echo "=========================================="
echo "Regression tests completed"
echo "=========================================="
echo "Results: $RESULTS_DIR/"
echo ""
ls -lh "$RESULTS_DIR"/ | grep -E "(memory-monitor|\.log)" || echo "No logs found"

