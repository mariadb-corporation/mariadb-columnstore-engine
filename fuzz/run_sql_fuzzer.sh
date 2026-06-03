#!/bin/bash
# MariaDB Columnstore SQL Mutation Fuzzer — launcher (run with --help)

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
FUZZ_PATH=$(realpath "$SCRIPT_LOCATION")
COLUMNSTORE_SOURCE_PATH=$(realpath "$SCRIPT_LOCATION"/..)

usage() {
    cat <<'EOF'
Usage: bash fuzz/run_sql_fuzzer.sh [sql_fuzzer.py args]

MariaDB Columnstore SQL Mutation Fuzzer — launcher

How it works:

  1. SEED   — Extracts ~75 k SQL statements from Columnstore MTR .test files.
  2. MUTATE — For each seed, applies 1-5 random weighted mutations
              (literals, predicates, window functions, aggregates, JOINs,
              subqueries, DDL, session variables, cross-engine ops, etc.).
  3. RUN    — Sends mutated SQL to a live server via a persistent `mariadb`
              client subprocess with per-query timeout.
  4. DETECT — Classifies each result: OK / error / hang / crash / assertion.
  5. DEDUP  — Two-layer dedup (exact + coarse) caps artifacts per error pattern.
  6. REPORT — Saves .sql repro files, crash artifacts (logs, cores, dmesg),
              summary.txt, and periodic checkpoints.

  On crash the fuzzer auto-restarts the server (up to 15 s wait, then
  systemctl restart).  Every 2000 queries it recreates fuzz_db to undo
  schema damage from ALTER/DROP mutations.

Prerequisites:
  - python3, mariadb (or mysql) client in PATH
  - a running MariaDB/Columnstore instance (socket or host/port)
  - auth via MYSQL_PWD, MCS_FUZZER_PASSWORD, or existing client config
  - recommended: sudo -n for crash recovery, mcsadmin for CS restart

Examples:
  # Local socket (most common):
  MYSQL_PWD='secret' MCS_FUZZ_SOCKET=/run/mysqld/mysqld.sock \
    bash fuzz/run_sql_fuzzer.sh --duration 300 --seed 42

  # TCP connection:
  MYSQL_PWD='secret' MCS_FUZZ_HOST=10.0.0.5 MCS_FUZZ_PORT=3306 \
    bash fuzz/run_sql_fuzzer.sh --duration 3600 --user fuzzer

Environment variables (CLI args override these):
  MCS_FUZZ_DURATION        default: 60
  MCS_FUZZ_SOCKET          preferred local socket
  MCS_FUZZ_HOST            TCP fallback if socket is not set
  MCS_FUZZ_PORT            default: 3306
  MCS_FUZZ_USER            default: fuzzer
  MCS_FUZZ_OUTPUT          default: fuzz/fuzz_results/sql_fuzzer
  MCS_FUZZ_QUERY_TIMEOUT   default: 10
  MCS_FUZZ_MAX_QUERIES     optional cap on total queries
  MCS_FUZZ_TEST_DIRS       space-separated .test directories
  MCS_FUZZER_PASSWORD      copied into MYSQL_PWD if unset

Notes:
  - The fuzzer recreates fuzz_db; use a disposable dev instance.
  - For Python-level args: python3 fuzz/sql_fuzzer.py --help
  - Triage: ignore obvious noise, but keep realistic repros even if a
    similar Jira is Fixed — it may be a regression.
EOF
}

preflight_checks() {
    if ! command -v python3 >/dev/null 2>&1; then
        echo "python3 not found in PATH" >&2
        exit 1
    fi
    if ! command -v mariadb >/dev/null 2>&1 && ! command -v mysql >/dev/null 2>&1; then
        echo "Neither mariadb nor mysql client was found in PATH" >&2
        exit 1
    fi
    if [[ ! -f "$FUZZ_PATH/sql_fuzzer.py" ]]; then
        echo "Missing $FUZZ_PATH/sql_fuzzer.py" >&2
        exit 1
    fi
}

SHOW_HELP=false
USER_SUPPLIED_CONNECTION=false
USER_SUPPLIED_PASSWORD=false
MASK_NEXT_PASSWORD=false
MASKED_ARGS=()

for arg in "$@"; do
    if [[ "$MASK_NEXT_PASSWORD" == "true" ]]; then
        MASKED_ARGS+=("***")
        MASK_NEXT_PASSWORD=false
        continue
    fi
    case "$arg" in
        -h|--help)
            SHOW_HELP=true
            MASKED_ARGS+=("$arg")
            ;;
        --socket|--host|--port)
            USER_SUPPLIED_CONNECTION=true
            MASKED_ARGS+=("$arg")
            ;;
        --socket=*|--host=*|--port=*)
            USER_SUPPLIED_CONNECTION=true
            MASKED_ARGS+=("$arg")
            ;;
        --password)
            USER_SUPPLIED_PASSWORD=true
            MASKED_ARGS+=("$arg")
            MASK_NEXT_PASSWORD=true
            ;;
        --password=*)
            USER_SUPPLIED_PASSWORD=true
            MASKED_ARGS+=("--password=***")
            ;;
        *)
            MASKED_ARGS+=("$arg")
            ;;
    esac
done

if [[ "$SHOW_HELP" == "true" ]]; then
    usage
    exit 0
fi

preflight_checks

if [[ ${#MASKED_ARGS[@]} -eq 0 ]]; then
    echo "Arguments received:"
else
    printf 'Arguments received:'
    printf ' %s' "${MASKED_ARGS[@]}"
    printf '\n'
fi

if [[ -n "${MCS_FUZZER_PASSWORD:-}" && -z "${MYSQL_PWD:-}" ]]; then
    export MYSQL_PWD="$MCS_FUZZER_PASSWORD"
fi

DEFAULT_ARGS=(
    --duration "${MCS_FUZZ_DURATION:-60}"
    --user "${MCS_FUZZ_USER:-fuzzer}"
    --output "${MCS_FUZZ_OUTPUT:-$COLUMNSTORE_SOURCE_PATH/fuzz/fuzz_results/sql_fuzzer}"
    --query-timeout "${MCS_FUZZ_QUERY_TIMEOUT:-10}"
)

if [[ "$USER_SUPPLIED_CONNECTION" != "true" ]]; then
    if [[ -n "${MCS_FUZZ_SOCKET:-}" ]]; then
        DEFAULT_ARGS+=(--socket "$MCS_FUZZ_SOCKET")
    elif [[ -S /run/mysqld/mysqld.sock ]]; then
        DEFAULT_ARGS+=(--socket /run/mysqld/mysqld.sock)
    elif [[ -n "${MCS_FUZZ_HOST:-}" ]]; then
        DEFAULT_ARGS+=(--host "$MCS_FUZZ_HOST" --port "${MCS_FUZZ_PORT:-3306}")
    fi
fi

if [[ -n "${MCS_FUZZ_MAX_QUERIES:-}" ]]; then
    DEFAULT_ARGS+=(--max-queries "$MCS_FUZZ_MAX_QUERIES")
fi

if [[ -n "${MCS_FUZZ_TEST_DIRS:-}" ]]; then
    read -r -a TEST_DIRS_ARRAY <<< "${MCS_FUZZ_TEST_DIRS}"
    DEFAULT_ARGS+=(--test-dirs "${TEST_DIRS_ARRAY[@]}")
fi

if [[ -z "${MYSQL_PWD:-}" && -z "${MCS_FUZZER_PASSWORD:-}" && "$USER_SUPPLIED_PASSWORD" != "true" ]]; then
    echo "MYSQL_PWD/MCS_FUZZER_PASSWORD is not set; relying on existing MariaDB client auth" >&2
fi

echo "Running SQL fuzzer from ${FUZZ_PATH}/sql_fuzzer.py"
exec python3 "$FUZZ_PATH/sql_fuzzer.py" "${DEFAULT_ARGS[@]}" "$@"
