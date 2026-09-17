#!/bin/bash

# Generate Woodpecker CI YAML files from jsonnet source
# For Woodpecker 3.x - generates separate files in .woodpecker/ directory

set -e

SCRIPT_DIR=$(dirname "$0")
CS_DIR=$(realpath "$SCRIPT_DIR/../..")
WOODPECKER_CLI_VERSION="v3.12.0"

CHECK_MODE=0
if [[ $# -eq 1 && "$1" == "--check" ]]; then
    CHECK_MODE=1
elif [[ $# -ne 0 ]]; then
    echo "Usage: $0 [--check]" >&2
    exit 2
fi

cd "$CS_DIR"

# Check dependencies
if ! command -v jsonnet &>/dev/null; then
    echo "Error: jsonnet is not installed"
    echo "Install with: apt install jsonnet"
    exit 1
fi

if ! python3 -c "import yaml" 2>/dev/null; then
    echo "Error: PyYAML is not installed"
    echo "Install with: pip3 install pyyaml"
    exit 1
fi

# Install woodpecker-cli if not available.
# Kept out of /usr/local/bin on purpose: this runs on developer machines as
# well as CI, and requiring sudo for a lint helper is a needless barrier.
WOODPECKER_CLI="woodpecker-cli"

install_woodpecker_cli() {
    if command -v woodpecker-cli &>/dev/null; then
        return 0
    fi

    echo "Installing woodpecker-cli ${WOODPECKER_CLI_VERSION}..."

    local arch
    if [[ "$(uname -m)" == "x86_64" ]]; then
        arch="amd64"
    else
        arch="arm64"
    fi

    # No --retry-all-errors: it needs curl >= 7.71 and plain --retry already
    # covers the transient network failures this download actually hits.
    curl -fsSL --retry 5 --retry-delay 1 "https://github.com/woodpecker-ci/woodpecker/releases/download/${WOODPECKER_CLI_VERSION}/woodpecker-cli_linux_${arch}.tar.gz" | tar -xz -C /tmp
    WOODPECKER_CLI="/tmp/woodpecker-cli"
    echo "woodpecker-cli installed: $("$WOODPECKER_CLI" --version)"
}

# Generate JSON from jsonnet and create separate files for each workflow
generate_ymls() {
    local outdir=$1

    mkdir -p "$outdir"

    # Drop any previously generated file first: without this, renaming or
    # removing a matrix entry leaves its stale .woodpecker/<old>.yml behind and
    # Woodpecker happily keeps running that removed workflow. --check reports it
    # as EXTRA, but only after it has already been committed.
    rm -f "$outdir"/*.yml

    jsonnet .woodpecker.jsonnet | python3 -c "
import sys
import json
import yaml
import os

outdir = sys.argv[1]
data = json.load(sys.stdin)

# Handle both single pipeline and array of pipelines
if isinstance(data, list):
    pipelines = data
else:
    pipelines = [data]

# Create separate file for each workflow
for pipeline in pipelines:
    name = pipeline.pop('name', 'pipeline')
    filename = os.path.join(outdir, f'{name}.yml')

    with open(filename, 'w') as f:
        yaml.dump(pipeline, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f'Generated: {filename}')
" "$outdir"
}

check_ymls() {
    local gendir=$1

    echo "Regenerating from .woodpecker.jsonnet into $gendir..."
    generate_ymls "$gendir"

    shopt -s nullglob
    local rc=0 f base
    for f in "$gendir"/*.yml; do
        base=$(basename "$f")
        if [[ ! -f ".woodpecker/$base" ]]; then
            echo "MISSING: $base is generated but missing from .woodpecker/"
            rc=1
        elif ! diff -u ".woodpecker/$base" "$f"; then
            echo "STALE: .woodpecker/$base differs from the generated output (see diff above)"
            rc=1
        fi
    done
    for f in .woodpecker/*.yml; do
        base=$(basename "$f")
        if [[ ! -f "$gendir/$base" ]]; then
            echo "EXTRA: $f exists but is not generated from .woodpecker.jsonnet"
            rc=1
        fi
    done
    shopt -u nullglob

    if [[ $rc -ne 0 ]]; then
        echo ""
        echo ".woodpecker/*.yml is out of sync with .woodpecker.jsonnet."
        echo "Run ./scripts/ci/generate_woodpecker.sh and commit the result."
        exit 1
    fi

    echo ".woodpecker/*.yml is in sync with .woodpecker.jsonnet"
}

if [[ $CHECK_MODE -eq 1 ]]; then
    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' EXIT
    check_ymls "$tmpdir"
    exit 0
fi

install_woodpecker_cli

echo "Generating Woodpecker CI config from .woodpecker.jsonnet..."
generate_ymls .woodpecker

echo ""
echo "Validating generated configurations..."

LINT_FAILED=0
for f in .woodpecker/*.yml; do
    echo -n "  Checking $(basename "$f")... "
    if "$WOODPECKER_CLI" lint "$f" 2>&1 | grep -q "Config is valid"; then
        echo "✅"
    else
        echo "❌"
        "$WOODPECKER_CLI" lint "$f"
        LINT_FAILED=1
    fi
done

echo ""
if [[ $LINT_FAILED -eq 1 ]]; then
    echo "❌ Some configurations have errors!"
    exit 1
fi

echo "✅ All configurations are valid!"
echo ""
ls -la .woodpecker/
