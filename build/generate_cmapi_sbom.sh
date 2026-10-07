#!/bin/bash

# Generates a CycloneDX 1.6 SBOM for CMAPI in three validated phases:
#
#   Phase A -- cpython part:
#       syft scans cmapi/python, we inject the parent cpython component, then
#       patch_cmapi_sbom_pbs.py adds libraries PBS statically links into the
#       interpreter (openssl, sqlite, zlib, libffi, ...). Validated in isolation.
#
#   Phase B -- deps part:
#       syft scans cmapi/deps (pip-installed Python packages). Validated.
#
#   Phase C -- merge:
#       Combine the two validated parts under a cmapi root component and inject
#       build metadata (arch, distro, git revision). Validate the final result.
#
# Isolating phases means a failure in e.g. the PBS patcher never contaminates
# the deps side, and the error message points at exactly one artifact.

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
CDXCLI_VERSION="${CDXCLI_VERSION:-0.27.2}"

select_pkg_format "$OS"

# CMAPI version = Columnstore version (see cmapi/CMakeLists.txt).
source "$COLUMNSTORE_SOURCE_PATH/VERSION"
CMAPI_VERSION="${COLUMNSTORE_VERSION_MAJOR}.${COLUMNSTORE_VERSION_MINOR}.${COLUMNSTORE_VERSION_PATCH}-${COLUMNSTORE_VERSION_RELEASE}"

case "$ARCH" in
  x86_64)          ARCH_SLUG="amd64" ;;
  arm64|aarch64)   ARCH_SLUG="arm64" ;;
  *)               ARCH_SLUG="$ARCH" ;;
esac

# Distro slug for the filename. On RPM distros we build on Rocky but tag the
# SBOM as rhel-<major> (rockylinux:8 -> rhel-8). On DEB distros we keep the
# full distro-version pair (ubuntu:22.04 -> ubuntu-22.04, debian:12 -> debian-12).
if [[ "$PKG_FORMAT" == "rpm" ]]; then
  DISTRO_SLUG="rhel-${OS##*:}"       # strip up to the colon, keep major version
  DISTRO_SLUG="${DISTRO_SLUG%%.*}"    # drop any ".minor" if present
else
  DISTRO_SLUG="$(echo "$OS" | tr ':' '-')"
fi

# Final merged SBOM; also the file naming convention consumed by createrepo.sh.
SBOM_OUT="$CMAPI_DIR/mariadb-columnstore-cmapi-${CMAPI_VERSION}-${DISTRO_SLUG}-${ARCH_SLUG}-${PKG_FORMAT}s.cdx.json"

# Intermediate per-phase artifacts, kept alongside the final file so CI logs
# have them if validation fails. Removed on success.
CPYTHON_PART="$CMAPI_DIR/.sbom-cpython-part.cdx.json"
DEPS_PART="$CMAPI_DIR/.sbom-deps-part.cdx.json"

cmapi_python_release_for_arch


# ---------------------------------------------------------------------------
# Installers
# ---------------------------------------------------------------------------

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

install_cyclonedx_cli() {
  if command -v cyclonedx >/dev/null 2>&1; then
    echo "cyclonedx-cli already present"
    return
  fi
  local dl_arch
  case "$ARCH" in
    x86_64)         dl_arch="linux-x64" ;;
    arm64|aarch64)  dl_arch="linux-arm64" ;;
    *)              echo "cyclonedx-cli: unsupported arch $ARCH"; exit 1 ;;
  esac
  echo "Installing cyclonedx-cli ${CDXCLI_VERSION}..."
  curl -sSfL -o /usr/local/bin/cyclonedx \
    "https://github.com/CycloneDX/cyclonedx-cli/releases/download/v${CDXCLI_VERSION}/cyclonedx-${dl_arch}"
  chmod +x /usr/local/bin/cyclonedx
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

# cyclonedx-cli is a single-file .NET binary; libicu isn't present on all our
# base images and the tool only produces ASCII output, so disable globalization.
# See https://aka.ms/dotnet-missing-libicu.
cdx_validate() {
  local file="$1"
  DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 cyclonedx validate \
    --input-file "$file" \
    --input-format json \
    --input-version v1_6 \
    --fail-on-errors
}

# Run a jq expression and fail the build with a labelled diagnostic if the
# expression returns a non-empty string. Used for semantic invariants that are
# cheaper and clearer to express in jq than in a dedicated validator.
cdx_assert() {
  local label="$1" file="$2" expr="$3"
  local report
  report=$(jq -r "$expr" "$file")
  if [[ -n "$report" ]]; then
    echo "SBOM validation failed [$label]:" >&2
    echo "$report" >&2
    exit 1
  fi
}


# ---------------------------------------------------------------------------
# Phase A -- cpython part
# ---------------------------------------------------------------------------

build_cpython_part() {
  if [[ ! -d "$CMAPI_DIR/python" ]]; then
    echo "cmapi/python not found; run build_cmapi.sh first."
    exit 1
  fi
  echo "[A] Scanning cmapi/python with syft..."
  syft scan -q -o cyclonedx-json@1.6 "dir:$CMAPI_DIR/python" > "$CPYTHON_PART.raw"

  echo "[A] Injecting cpython parent component..."
  # bom-ref matches what patch_cmapi_sbom_pbs.py auto-detects via find_cpython_component.
  jq \
    --arg version "$PBS_CPYTHON_VERSION" \
    --arg url     "$PBS_URL" \
    --arg sha     "$PBS_SHA" \
    --arg release "$PBS_RELEASE" \
    '
    .["$schema"] = "http://cyclonedx.org/schema/bom-1.6.schema.json" |
    .specVersion = "1.6" |
    .components = ((.components // []) + [{
      type: "application",
      "bom-ref": ("pkg:generic/cpython@" + $version + "?release=" + $release),
      name: "cpython",
      version: $version,
      description: "python-build-standalone portable CPython interpreter bundled with CMAPI",
      purl: ("pkg:generic/cpython@" + $version + "?release=" + $release),
      supplier: { name: "Astral (python-build-standalone)" },
      externalReferences: [
        { type: "distribution", url: $url, hashes: [{ alg: "SHA-256", content: $sha }] },
        { type: "vcs",          url: "https://github.com/astral-sh/python-build-standalone" }
      ],
      hashes: [ { alg: "SHA-256", content: $sha } ]
    }])
    ' "$CPYTHON_PART.raw" > "$CPYTHON_PART"
  rm -f "$CPYTHON_PART.raw"

  patch_pbs_libraries "$CPYTHON_PART"

  echo "[A] Validating cpython part..."
  cdx_validate "$CPYTHON_PART"
  cdx_assert "cpython-part" "$CPYTHON_PART" '
    def pbs: [.components[]? | select(.properties // [] | any(.name == "pbs:release"))];
    def cpy: [.components[]? | select(.name == "cpython")];
    [
      (if (cpy | length) != 1
         then "expected exactly one cpython component, found \(cpy | length)" else empty end),
      (if (pbs | length) < 2
         then "expected cpython parent + at least one PBS library (patch_cmapi_sbom_pbs.py did nothing)"
         else empty end)
    ] | map(select(length > 0)) | join("\n")
  '
  echo "[A] cpython part OK: $CPYTHON_PART"
}

patch_pbs_libraries() {
  local target="$1"
  if [[ ! -f "$CMAPI_DIR/pp/PYTHON.json" ]]; then
    echo "cmapi/pp/PYTHON.json missing; PBS metadata not preserved by build_cmapi.sh."
    exit 1
  fi
  # build_cmapi.sh moves pp/install out to cmapi/python; recreate the link so
  # the patcher can also scan install/lib/*.so* (tcl/tk et al. ship as shared).
  ln -sfn "$CMAPI_DIR/python" "$CMAPI_DIR/pp/install"

  # Fetch downloads.json via host curl rather than the bundled PBS python:
  # PBS python's OpenSSL doesn't trust the system CA store on Rocky
  # (SSL: CERTIFICATE_VERIFY_FAILED). curl handles CAs correctly on all distros.
  local downloads_file="$CMAPI_DIR/pp/pbs-downloads.json"
  echo "[A] Fetching PBS downloads.json for release ${PBS_RELEASE}..."
  if ! curl -sSfL -o "$downloads_file" \
       "https://raw.githubusercontent.com/astral-sh/python-build-standalone/${PBS_RELEASE}/pythonbuild/downloads.json"; then
    curl -sSfL -o "${downloads_file%.json}.py" \
      "https://raw.githubusercontent.com/astral-sh/python-build-standalone/${PBS_RELEASE}/pythonbuild/downloads.py"
    downloads_file="${downloads_file%.json}.py"
  fi

  echo "[A] Patching with PBS-bundled libraries..."
  "$CMAPI_DIR/python/bin/python3" "$SCRIPT_LOCATION/patch_cmapi_sbom_pbs.py" \
    --sbom "$target" \
    --build "$CMAPI_DIR/pp" \
    --tag "$PBS_RELEASE" \
    --downloads "$downloads_file"

  rm -f "$CMAPI_DIR/pp/install" "$downloads_file"
}


# ---------------------------------------------------------------------------
# Phase B -- deps part
# ---------------------------------------------------------------------------

build_deps_part() {
  if [[ ! -d "$CMAPI_DIR/deps" ]]; then
    echo "cmapi/deps not found; run build_cmapi.sh first."
    exit 1
  fi
  echo "[B] Scanning cmapi/deps with syft..."
  syft scan -q -o cyclonedx-json@1.6 "dir:$CMAPI_DIR/deps" > "$DEPS_PART"

  echo "[B] Validating deps part..."
  cdx_validate "$DEPS_PART"
  cdx_assert "deps-part" "$DEPS_PART" '
    if (.components // []) | length == 0
      then "no Python packages discovered in cmapi/deps (requirements install broken?)"
      else empty end
  '
  echo "[B] deps part OK: $DEPS_PART"
}


# ---------------------------------------------------------------------------
# Phase C -- merge
# ---------------------------------------------------------------------------

merge_parts() {
  echo "[C] Merging cpython + deps parts under cmapi root..."
  # jq -s receives [cpython_part, deps_part] as the input array. unique_by
  # keeps the first occurrence of each bom-ref -- the cpython part comes first
  # so its enriched cpython component wins over any duplicate from deps.
  jq -s \
    --arg arch     "$ARCH" \
    --arg distro   "$OS" \
    --arg version  "$CMAPI_VERSION" \
    --arg revision "${DRONE_COMMIT:-${CMAPI_GIT_REVISION:-unknown}}" \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '
    ((.[0].components   // []) + (.[1].components   // [])) as $all_components |
    ((.[0].dependencies // []) + (.[1].dependencies // [])) as $all_deps |

    {
      "$schema":    "http://cyclonedx.org/schema/bom-1.6.schema.json",
      bomFormat:    "CycloneDX",
      specVersion:  "1.6",
      metadata: {
        timestamp: $timestamp,
        component: {
          type:      "application",
          "bom-ref": "mariadb-columnstore-cmapi",
          name:      "mariadb-columnstore-cmapi",
          version:   $version,
          supplier:  { name: "MariaDB Plc" }
        },
        properties: [
          { name: "cmapi:arch",         value: $arch     },
          { name: "cmapi:distro",       value: $distro   },
          { name: "cmapi:git_revision", value: $revision }
        ]
      },
      components:   ($all_components | unique_by(."bom-ref")),
      dependencies: $all_deps
    }
    ' "$CPYTHON_PART" "$DEPS_PART" > "$SBOM_OUT"
}


# ---------------------------------------------------------------------------
# Final validation
# ---------------------------------------------------------------------------

validate_final() {
  echo "[final] Schema validation..."
  cdx_validate "$SBOM_OUT"

  echo "[final] Invariant checks..."
  cdx_assert "final" "$SBOM_OUT" '
    def refs: [.components[]?."bom-ref"];
    def dep_refs: [.dependencies[]?.dependsOn[]?] + [.dependencies[]?.ref];
    def dupes(a): a | group_by(.) | map(select(length > 1) | .[0]);
    def dangling: dep_refs - refs;

    [
      (if ([.components[]? | select(.name == "cpython")] | length) != 1
         then "expected exactly one cpython component, found \([.components[]? | select(.name == "cpython")] | length)"
         else empty end),
      (if ([.components[]? | select(.properties // [] | any(.name == "pbs:release"))] | length) < 1
         then "no PBS-derived components present (cpython part merge dropped them)"
         else empty end),
      (if (dupes(refs) | length) > 0
         then "duplicate bom-refs: \(dupes(refs) | join(", "))"
         else empty end),
      (if (dangling | length) > 0
         then "dependency edges reference unknown bom-refs: \(dangling | join(", "))"
         else empty end),
      (if (.metadata.timestamp // "") == ""
         then "metadata.timestamp missing (NTIA minimum field)"
         else empty end),
      (if (.metadata.component.supplier.name // "") != "MariaDB Plc"
         then "root component supplier must be \"MariaDB Plc\", found \"\(.metadata.component.supplier.name // "")\""
         else empty end)
    ] | map(select(length > 0)) | join("\n")
  '
  echo "[final] OK: $SBOM_OUT"
}


# ---------------------------------------------------------------------------

# On success remove the per-phase intermediates; on failure keep them so CI
# logs can inspect exactly which phase produced the broken SBOM.
cleanup_intermediates_on_success() {
  if [[ $? -eq 0 ]]; then
    rm -f "$CPYTHON_PART" "$DEPS_PART"
  else
    echo "SBOM generation failed; intermediates kept at:" >&2
    echo "  $CPYTHON_PART" >&2
    echo "  $DEPS_PART" >&2
  fi
}
trap cleanup_intermediates_on_success EXIT

install_prereqs
install_syft
install_cyclonedx_cli

build_cpython_part
build_deps_part
merge_parts
validate_final
