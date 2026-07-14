#!/usr/bin/env bash
#
# Run Infer (SAST) over the MariaDB ColumnStore code ONLY and fail if any
# ticket-worthy finding remains.
#
# This mirrors the ES Jenkins pipeline (and the Confluence "Key Points FAQ",
# Appendix II) but restricts the analysis to storage/columnstore/* translation
# units.
#
# It is meant to run *after* the CI `clone-mdb` step, i.e. inside a full
# MariaDB server tree that already has this ColumnStore checkout copied into
# storage/columnstore/columnstore. The server root is derived from this
# script's location (../../../..), exactly like bootstrap_mcs.sh, or can be
# overridden with SRC_DIR.
#
# Requirements: x86_64 Linux with glibc >= 2.38 (Infer 1.3 needs it -> use
# ubuntu:24.04 / debian:13, NOT ubuntu:22.04 or Rocky 8/9).
#
# Usage (as invoked from .drone.jsonnet):
#   run_infer.sh --distro ubuntu:24.04 --install-deps --result-dir /drone/src/infer
#
set -o pipefail

# ---------- Script / source locations ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# build/ -> columnstore/ -> columnstore/ -> storage/ -> <server root>
SERVER_ROOT="${SRC_DIR:-$(realpath "$SCRIPT_DIR/../../../..")}"

# ---------- Parameters (flags override env, env overrides defaults) ----------
DISTRO="${DISTRO:-}"
INSTALL_DEPS=false
RESULT_DIR="${RESULT_DIR:-}"
SERVER="${SERVER:-11.8-enterprise}"
INFER_VERSION="${INFER_VERSION:-1.3.0}"
CLANG_VERSION="${CLANG_VERSION:-21}"           # clang bundled with Infer 1.3.0
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN)}"
FULL_CAPTURE="${FULL_CAPTURE:-0}"              # 1 = capture whole server (CI parity, slow)
WORKSPACE="${WORKSPACE:-/mdb/infer}"           # scratch on the shared CI volume

while [ $# -gt 0 ]; do
  case "$1" in
    --distro) DISTRO="$2"; shift 2 ;;
    --server) SERVER="$2"; shift 2 ;;
    --result-dir) RESULT_DIR="$2"; shift 2 ;;
    --jobs) JOBS="$2"; shift 2 ;;
    --install-deps) INSTALL_DEPS=true; shift ;;
    --full-capture) FULL_CAPTURE=1; shift ;;
    *) echo "ERROR: unknown argument '$1'"; exit 2 ;;
  esac
done

INFER_BIN="$WORKSPACE/infer-bin"
BUILD="$WORKSPACE/build"
OUT="$WORKSPACE/out"                            # infer results dir
INFERCONFIG_SRC="$SCRIPT_DIR/security/inferconfig.json"

log() { echo "== [run_infer] $*"; }

mkdir -p "$WORKSPACE"

# ---------- 0. Sanity checks ----------
[ "$(uname -m)" = "x86_64" ] || { echo "ERROR: need x86_64, got $(uname -m)"; exit 1; }
[ -d "$SERVER_ROOT/storage/columnstore/columnstore" ] || {
  echo "ERROR: $SERVER_ROOT does not look like a server tree with ColumnStore"; exit 1; }
[ -f "$INFERCONFIG_SRC" ] || { echo "ERROR: missing $INFERCONFIG_SRC"; exit 1; }
log "Server root : $SERVER_ROOT"
log "Distro      : ${DISTRO:-<unset>}"
log "Server      : $SERVER"
log "glibc       : $(ldd --version | head -1)"   # must be >= 2.38 for Infer 1.3

# ---------- 1. Build dependencies ----------
# Mirrors the deb list from bootstrap_mcs.sh:install_deps plus the few tools this
# script needs directly (wget/xz/jq). Runner is pinned to ubuntu:24.04 (deb).
if [ "$INSTALL_DEPS" = true ]; then
  log "Installing build dependencies"
  export DEBIAN_FRONTEND=noninteractive
  DEB_BUILD_DEPS="build-essential automake libboost-all-dev bison cmake \
    libncurses-dev python3 libaio-dev libsystemd-dev libpcre2-dev libperl-dev \
    libssl-dev libxml2-dev libkrb5-dev flex libpam-dev git libsnappy-dev \
    libcurl4-openssl-dev libgtest-dev libcppunit-dev googletest libjemalloc-dev \
    liblz-dev liblzo2-dev liblzma-dev liblz4-dev libbz2-dev libbenchmark-dev \
    libdistro-info-perl graphviz devscripts ccache equivs eatmydata curl python3"
  apt-get -y update || { echo "ERROR: apt-get update failed"; exit 3; }
  # shellcheck disable=SC2086
  apt-get -y install $DEB_BUILD_DEPS wget xz-utils jq \
    || { echo "ERROR: installing build dependencies failed"; exit 3; }
fi

for t in git cmake wget tar xz python3 jq; do
  command -v "$t" >/dev/null || { echo "ERROR: missing required tool '$t'"; exit 1; }
done

# ---------- 2. INFERCONFIG (censor rules shared with the ES Jenkins pipeline) ----------
export INFERCONFIG="$INFERCONFIG_SRC"
log "Using INFERCONFIG=$INFERCONFIG"

# ---------- 3. Download Infer ----------
if [ ! -x "$INFER_BIN/bin/infer" ]; then
  log "Downloading Infer $INFER_VERSION"
  tarball="infer-linux-x86_64-v${INFER_VERSION}.tar.xz"
  wget -nv "https://github.com/facebook/infer/releases/download/v${INFER_VERSION}/${tarball}" \
    || { echo "ERROR: failed to download Infer $INFER_VERSION"; exit 5; }
  mkdir -p "$INFER_BIN"
  tar -xf "$tarball" -C "$INFER_BIN" --strip-components=1 \
    || { echo "ERROR: failed to extract Infer $INFER_VERSION"; exit 5; }
  rm -f "$tarball"
fi

# ---------- 4. PATH workaround (MENT-2571): Infer's bundled clang must come first ----------
export PATH="$INFER_BIN/bin:$INFER_BIN/lib/infer/facebook-clang-plugins/clang/install/bin:$PATH"
CLANG="$INFER_BIN/lib/infer/facebook-clang-plugins/clang/install/bin/clang-${CLANG_VERSION}"
CLANGpp="$INFER_BIN/lib/infer/facebook-clang-plugins/clang/install/bin/clang++"
infer --version || { echo "ERROR: infer is not runnable (glibc < 2.38 or bad download?)"; exit 5; }

# ---------- 5. Configure with Infer's clang + ColumnStore as a dynamic plugin ----------
case "$SERVER" in
  *enterprise) BUILD_CONFIG=enterprise ;;
  *) BUILD_CONFIG=mysql_release ;;
esac
log "cmake BUILD_CONFIG=$BUILD_CONFIG"
rm -rf "$BUILD"
# Plugin/feature disables mirror bootstrap_mcs.sh:construct_cmake_flags. They are
# required (not just an optimization): the CI clone-mdb step clones the server
# with the wsrep-lib and rocksdb submodules skipped, so WSREP/ROCKSDB must be
# turned off or cmake aborts ("No MariaDB wsrep-API code!"). WITH_SSL=system also
# avoids the bundled-OpenSSL link issues.
cmake -DBUILD_CONFIG="$BUILD_CONFIG" \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DPLUGIN_COLUMNSTORE=DYNAMIC \
      -DPLUGIN_CONNECT=NO \
      -DPLUGIN_GSSAPI=NO \
      -DPLUGIN_MROONGA=NO \
      -DPLUGIN_OQGRAPH=NO \
      -DPLUGIN_ROCKSDB=NO \
      -DPLUGIN_SPHINX=NO \
      -DPLUGIN_SPIDER=NO \
      -DPLUGIN_TOKUDB=NO \
      -DWITH_EMBEDDED_SERVER=NO \
      -DWITH_SSL=system \
      -DWITH_WSREP=NO \
      -DUPDATE_SUBMODULES=OFF \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
      -DCMAKE_C_COMPILER="$CLANG" -DCMAKE_CXX_COMPILER="$CLANGpp" \
      -S "$SERVER_ROOT" -B "$BUILD" \
  || { echo "ERROR: cmake configure failed"; exit 4; }

# ---------- 6. Build (generated headers that ColumnStore TUs #include must exist) ----------
# shellcheck disable=SC1091
. "$SERVER_ROOT/VERSION"
ver="${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}"
if [ "$ver" = "10.6" ]; then targets="GenError GenServerSource GenFixPrivs"
else targets="GenError GenServerSource GenUnicodeDataSource GenFixPrivs"; fi
# Build as much as possible. Some ColumnStore *test* executables can fail to
# link -- Infer does not need them, so keep going (-k) instead of aborting.
cmake --build "$BUILD" --parallel "$JOBS" -- -k \
  || log "WARN: some targets failed to build (likely test binaries) -- continuing for Infer"
# Generated sources the ColumnStore TUs include -- these must succeed:
# shellcheck disable=SC2086
cmake --build "$BUILD" --target $targets --parallel "$JOBS" \
  || { echo "ERROR: building generated targets ($targets) failed"; exit 4; }

# ---------- 7. ColumnStore-only compilation database ----------
cd "$BUILD" || { echo "ERROR: failed to cd into $BUILD"; exit 4; }
[ -f compile_commands.json ] || { echo "ERROR: no compile_commands.json in $BUILD"; exit 4; }
if [ "$FULL_CAPTURE" = "1" ]; then
  CAPTURE_DB="compile_commands.json"
  log "FULL_CAPTURE=1 -> capturing the whole server (CI parity, slow)"
else
  CAPTURE_DB="compile_commands.columnstore.json"
  # Resolve each entry's "file" relative to its own "directory" (compile DB
  # paths may be relative) before matching, so no ColumnStore TU is missed.
  python3 - <<'PY' || { echo "ERROR: filtering compile DB failed"; exit 4; }
import json, os
with open("compile_commands.json", encoding="utf-8") as fh:
    db = json.load(fh)
def abspath(e):
    f = e["file"]
    return os.path.realpath(f if os.path.isabs(f) else os.path.join(e.get("directory", ""), f))
cs = [e for e in db if "/storage/columnstore/" in abspath(e)]
with open("compile_commands.columnstore.json", "w", encoding="utf-8") as fh:
    json.dump(cs, fh, indent=1)
print(f"== [run_infer] ColumnStore TUs: {len(cs)} / {len(db)} total")
PY
fi

# ---------- 8. Capture + analyze ----------
rm -rf "$OUT"
infer capture  --keep-going --compilation-database "$CAPTURE_DB" --project-root "$SERVER_ROOT" --results-dir "$OUT" \
  || { echo "ERROR: infer capture failed"; exit 5; }
infer analyze  --keep-going                                      --project-root "$SERVER_ROOT" --results-dir "$OUT" \
  || { echo "ERROR: infer analyze failed"; exit 5; }
[ -f "$OUT/report.json" ] || { echo "ERROR: infer produced no report.json"; exit 5; }

# ---------- 9. Report + gate ----------
ISSUES="$OUT/columnstore_issues.txt"
# Ticket-worthy = ColumnStore files, dropping:
#   * censored issues     (config-driven, via inferconfig.json "censor-report")
#   * suppressed issues    (source-driven, via `// @infer-ignore` comments;
#                           Infer keeps these in report.json with "suppressed":true
#                           and only removes them from report.txt)
# This mirrors the ES inferTickets.groovy stage.
jq -r '.[]
       | select(has("censored_reason") | not)
       | select(.suppressed != true)
       | select(.file | test("storage/columnstore/"))
       | "\(.file):\(.line): \(.bug_type): \(.qualifier)"' \
   "$OUT/report.json" | sort > "$ISSUES"

count="$(wc -l <"$ISSUES" | tr -d ' ')"

echo
echo "==================== Infer ColumnStore summary ===================="
echo "Full report (incl. censored): $OUT/report.txt"
echo "Raw JSON                    : $OUT/report.json"
echo "Ticket-worthy CS issues     : $ISSUES ($count)"
echo "Count by bug type:"
jq -r '.[] | select(has("censored_reason")|not) | select(.suppressed != true) | select(.file|test("storage/columnstore/")) | .bug_type' \
   "$OUT/report.json" | sort | uniq -c | sort -rn || true
echo "-------------------------------------------------------------------"
[ "$count" -gt 0 ] && { echo "Findings:"; cat "$ISSUES"; }
echo "==================================================================="

# Copy artifacts to the workspace result dir so a later step can publish them.
if [ -n "$RESULT_DIR" ]; then
  mkdir -p "$RESULT_DIR"
  cp -f "$OUT/report.txt" "$OUT/report.json" "$ISSUES" "$RESULT_DIR/" 2>/dev/null || true
  log "Artifacts copied to $RESULT_DIR"
fi

if [ "$count" -gt 0 ]; then
  echo "FAILURE: Infer found $count ticket-worthy issue(s) in ColumnStore code."
  exit 1
fi
log "OK: no ticket-worthy Infer issues in ColumnStore code."
