#!/bin/bash

# Generates a CycloneDX SBOM for CMAPI, covering:
#   * Python packages installed into cmapi/deps (from requirements.txt)
#   * The bundled python-build-standalone interpreter extracted to cmapi/python
#
# Runs after build_cmapi.sh has populated cmapi/deps and cmapi/python.
# Emits cmapi/cmapi-sbom-${arch}.cdx.json (CycloneDX 1.5 JSON).

set -eo pipefail

SCRIPT_LOCATION=$(dirname "$0")
COLUMNSTORE_SOURCE_PATH=$(realpath "$SCRIPT_LOCATION"/../)
CMAPI_DIR="$COLUMNSTORE_SOURCE_PATH/cmapi"

source "$SCRIPT_LOCATION"/utils.sh
source "$SCRIPT_LOCATION"/cmapi_python_release.sh

optparse.define short=d long=distro desc="distro" variable=OS
source $(optparse.build)

if [[ -z "${OS:-}" ]]; then
  echo "Please provide --distro parameter, e.g. ./generate_cmapi_sbom.sh --distro ubuntu:22.04"
  exit 1
fi

ARCH="$(arch)"
SYFT_VERSION="${SYFT_VERSION:-1.18.1}"

select_pkg_format "$OS"

# CMAPI version = Columnstore version (see cmapi/CMakeLists.txt).
source "$COLUMNSTORE_SOURCE_PATH/VERSION"
CMAPI_VERSION="${COLUMNSTORE_VERSION_MAJOR}.${COLUMNSTORE_VERSION_MINOR}.${COLUMNSTORE_VERSION_PATCH}-${COLUMNSTORE_VERSION_RELEASE}"

# Filename convention: <product>-<version>-<arch>-<fmt>s.cdx.json
#   e.g. mariadb-columnstore-cmapi-25.10.7-1-amd64-debs.cdx.json
case "$ARCH" in
  x86_64)          ARCH_SLUG="amd64" ;;
  arm64|aarch64)   ARCH_SLUG="arm64" ;;
  *)               ARCH_SLUG="$ARCH" ;;
esac
SBOM_OUT="$CMAPI_DIR/mariadb-columnstore-cmapi-${CMAPI_VERSION}-${ARCH_SLUG}-${PKG_FORMAT}s.cdx.json"

cmapi_python_release_for_arch

install_prereqs() {
  if [[ "$PKG_FORMAT" == "rpm" ]]; then
    # curl-minimal is preinstalled on Rocky and provides `curl`; installing the
    # full curl package conflicts with it. tar is also part of the base image.
    retry_eval 5 "dnf install -q -y jq"
  else
    retry_eval 5 "apt-get update -qq -o Dpkg::Use-Pty=0 && apt-get install -qq -o Dpkg::Use-Pty=0 curl jq ca-certificates"
  fi
}

install_syft() {
  if command -v syft >/dev/null 2>&1; then
    echo "syft already present: $(syft version | head -1)"
    return
  fi
  echo "Installing syft ${SYFT_VERSION}..."
  curl -sSfL https://raw.githubusercontent.com/anchore/syft/main/install.sh \
    | sh -s -- -b /usr/local/bin "v${SYFT_VERSION}"
}

generate_python_sbom() {
  if [[ ! -d "$CMAPI_DIR/deps" ]]; then
    echo "cmapi/deps not found; run build_cmapi.sh first."
    exit 1
  fi
  echo "Scanning cmapi/deps and cmapi/python with syft..."
  # -q suppresses progress; scan both python site-packages tree and interpreter tree.
  syft scan -q -o cyclonedx-json \
    "dir:$CMAPI_DIR/deps" \
    > "$SBOM_OUT.deps.json"

  syft scan -q -o cyclonedx-json \
    "dir:$CMAPI_DIR/python" \
    > "$SBOM_OUT.interp.json"
}

merge_and_augment() {
  # Merge the two syft outputs and inject an authoritative cpython component
  # (with the upstream release URL + sha256) so downstream consumers can trace
  # the bundled interpreter regardless of what syft found inside cmapi/python.
  local cpython_component
  cpython_component=$(jq -n \
    --arg version "$PBS_CPYTHON_VERSION" \
    --arg url "$PBS_URL" \
    --arg sha "$PBS_SHA" \
    --arg release "$PBS_RELEASE" \
    '{
       type: "application",
       "bom-ref": ("pkg:generic/cpython@" + $version + "?release=" + $release),
       name: "cpython",
       version: $version,
       description: "python-build-standalone portable CPython interpreter bundled with CMAPI",
       purl: ("pkg:generic/cpython@" + $version + "?release=" + $release),
       supplier: { name: "Astral (python-build-standalone)" },
       externalReferences: [
         { type: "distribution", url: $url },
         { type: "vcs", url: "https://github.com/astral-sh/python-build-standalone" }
       ],
       hashes: [ { alg: "SHA-256", content: $sha } ]
     }')

  local interp_components="[]"
  if [[ -s "$SBOM_OUT.interp.json" ]]; then
    interp_components=$(jq '.components // []' "$SBOM_OUT.interp.json")
  fi

  jq \
    --argjson cpython "$cpython_component" \
    --argjson interp "$interp_components" \
    --arg arch "$ARCH" \
    --arg distro "$OS" \
    --arg revision "${DRONE_COMMIT:-${CMAPI_GIT_REVISION:-unknown}}" \
    '
    .components = ((.components // []) + [$cpython] + $interp) |
    .metadata.component = {
      type: "application",
      name: "mariadb-columnstore-cmapi",
      "bom-ref": "mariadb-columnstore-cmapi"
    } |
    .metadata.properties = ((.metadata.properties // []) + [
      { name: "cmapi:arch", value: $arch },
      { name: "cmapi:distro", value: $distro },
      { name: "cmapi:git_revision", value: $revision }
    ])
    ' "$SBOM_OUT.deps.json" > "$SBOM_OUT"

  rm -f "$SBOM_OUT.deps.json" "$SBOM_OUT.interp.json"
  echo "SBOM written to: $SBOM_OUT"
}

install_prereqs
install_syft
generate_python_sbom
merge_and_augment
