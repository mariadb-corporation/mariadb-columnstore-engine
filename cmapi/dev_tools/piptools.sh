#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cmapi_dir="$(realpath "${script_dir}/..")"

export CUSTOM_COMPILE_COMMAND="dev_tools/piptools.sh compile-all"

ensure_piptools() {
	if ! command -v pip-compile >/dev/null 2>&1; then
		echo "Installing pip-tools..."
		python3 -m pip install --upgrade pip
		python3 -m pip install pip-tools
	fi
}

compile_runtime() {
	ensure_piptools
	cd "${cmapi_dir}"
	pip-compile --quiet --resolver=backtracking --output-file=requirements.txt requirements.in
}

compile_dev() {
	ensure_piptools
	cd "${cmapi_dir}"
	pip-compile --quiet --resolver=backtracking --output-file=requirements-dev.txt requirements-dev.in
}

compile_all() {
	compile_runtime
	compile_dev
}

sync_runtime() {
	ensure_piptools
	cd "${cmapi_dir}"
	pip-sync requirements.txt
}

sync_dev() {
	ensure_piptools
	cd "${cmapi_dir}"
	pip-sync requirements.txt requirements-dev.txt
}

usage() {
	cat <<EOF
Usage: dev_tools/piptools.sh <command>

Commands:
	compile-runtime   Compile requirements.in -> requirements.txt
	compile-dev       Compile requirements-dev.in -> requirements-dev.txt
	compile-all       Compile both runtime and dev requirements (default)
	sync-runtime      pip-sync runtime requirements only
	sync-dev          pip-sync runtime + dev requirements
	help              Show this help
EOF
}

cmd="${1:-compile-all}"
case "${cmd}" in
	compile-runtime) compile_runtime ;;
	compile-dev)     compile_dev ;;
	compile-all)     compile_all ;;
	sync-runtime)    sync_runtime ;;
	sync-dev)        sync_dev ;;
	help|--help|-h)  usage ;;
	*) echo "Unknown command: ${cmd}" >&2; usage; exit 1 ;;
esac

