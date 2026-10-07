#!/usr/bin/env python3
"""Patch a CycloneDX SBOM with python-build-standalone (PBS) bundled deps.

PBS statically links OpenSSL, SQLite, zlib, libffi, ncurses, ... into the
CPython distribution it ships, so syft/cyclonedx-py can only see the outer
interpreter -- not the libraries baked into it. This script closes that gap:

  1. Reads PYTHON.json from the unpacked PBS "full" archive to enumerate every
     library each extension module links against (source of truth: the build
     itself, not a hand-maintained list).
  2. Looks each linked library up in downloads.json / downloads.py at the
     matching PBS release tag on github.com/astral-sh/python-build-standalone
     to recover the upstream source URL, SHA-256, version and SPDX license.
  3. Emits CycloneDX 1.5 library components (with mime-type, purl, hashes,
     licenses, external references) and wires them under the CPython
     component as `dependsOn` edges.

Design notes:
  * Build-time-only components (autoconf, binutils, m4, patchelf) are dropped
    even if the algorithm reaches them -- they never ship at runtime.
  * A curated runtime allowlist is used only for a warning: anything outside
    it is still emitted but flagged in the log, so newly-added PBS libraries
    surface visibly instead of silently.
  * When a link name matches several downloads entries (openssl-1.1 vs
    openssl-3.5), the resolver picks the shortest key -- a deterministic
    tie-break that matches the convention that unsuffixed keys are canonical.

Approach adapted from https://github.com/astral-sh/python-build-standalone/issues/403
and https://gist.github.com/Edward-Knight/f23b65356216f94b0d92ae82d1f5354b.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.request
from pathlib import Path

RAW_URL = "https://raw.githubusercontent.com/astral-sh/python-build-standalone/{tag}/pythonbuild/{file}"
PBS_REPO_URL = "https://github.com/astral-sh/python-build-standalone"

# Components that only exist to build CPython (never present at runtime).
BUILD_TIME_ONLY = frozenset({"autoconf", "binutils", "m4", "patchelf"})

# Curated set of libraries PBS is known to bundle at runtime. Used only for
# a warning: anything discovered outside this set is still emitted.
KNOWN_RUNTIME = frozenset({
    "bdb", "bzip2", "cpython", "expat", "libedit", "libffi", "libuuid",
    "libX11", "libXau", "libxcb", "mpdecimal", "ncurses", "openssl",
    "sqlite", "tcl", "tix", "tk", "xorgproto", "xz", "zlib",
})

EXTENSION_TO_MIME = {
    ".bz2":  "application/x-bzip2",
    ".gz":   "application/gzip",
    ".tgz":  "application/gzip",
    ".xz":   "application/x-xz",
    ".zst":  "application/zstd",
    ".zip":  "application/zip",
}


# ---------------------------------------------------------------------------
# downloads.json / downloads.py
# ---------------------------------------------------------------------------

def _parse_downloads_py(src: str) -> dict:
    """Extract the DOWNLOADS literal without executing pythonbuild code."""
    for node in ast.parse(src).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "DOWNLOADS" for t in node.targets
        ):
            downloads = ast.literal_eval(node.value)
            break
    else:
        raise ValueError("no DOWNLOADS assignment in downloads.py")
    # Upstream URLs live in "# Mirrored from <url>" comments in the .py form.
    for key, url in re.findall(r'"([^"]+)":\s*\{\s*#\s*Mirrored from (\S+)', src):
        downloads.get(key, {}).setdefault("upstream_url", url)
    return downloads


def load_downloads(text: str, filename: str) -> dict:
    return json.loads(text) if filename.endswith(".json") else _parse_downloads_py(text)


def fetch_downloads(tag: str) -> tuple[dict, str]:
    """Fetch downloads.json (preferred) or downloads.py from the PBS release tag."""
    ctx = ssl.create_default_context()
    for fname in ("downloads.json", "downloads.py"):
        url = RAW_URL.format(tag=tag, file=fname)
        try:
            with urllib.request.urlopen(url, timeout=30, context=ctx) as resp:
                return load_downloads(resp.read().decode(), fname), url
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue
            raise
        except urllib.error.URLError as e:
            sys.exit(
                f"error: cannot fetch {url}: {e.reason}\n"
                f"hint: download it and pass --downloads FILE"
            )
    sys.exit(f"error: neither downloads.json nor downloads.py found at tag {tag!r}")


# ---------------------------------------------------------------------------
# Build inspection
# ---------------------------------------------------------------------------

def discover_linked_libs(python_json: dict, build_dir: Path) -> dict[str, set[str]]:
    """Map link name -> set of extension modules that pull it in.

    PBS marks libraries it ships itself as "system"; we still count those when
    the archive contains their .a / .so, because "system on the build host"
    still means "shipped in the tarball".
    """
    build_info = python_json["build_info"]
    users: dict[str, set[str]] = {}

    def has_library_file(name: str) -> bool:
        if (build_dir / "build" / "lib" / f"lib{name}.a").is_file():
            return True
        return any((build_dir / "install" / "lib").glob(f"lib{name}.so*"))

    def add(link: dict, user: str) -> None:
        name = link["name"]
        if not link.get("system") or has_library_file(name):
            users.setdefault(name, set()).add(user)

    for link in build_info.get("core", {}).get("links", []):
        add(link, "<core>")
    for ext_name, variants in build_info.get("extensions", {}).items():
        for variant in variants:
            for link in variant.get("links", []):
                add(link, ext_name)
    return users


def resolve_download_key(link_name: str, downloads: dict) -> str | None:
    """Pick the downloads.json key whose library_names cover `link_name`.

    Prefer exact matches on the fully-qualified name; fall back to suffix
    match (PBS calls Tk 9 "tcl9tk9.0" while downloads lists "tk9.0").
    Windows-only entries are skipped since we target Linux. Ties are broken
    by shortest key (unsuffixed = canonical: "openssl" over "openssl-1.1").
    """
    exact = [
        k for k, v in downloads.items()
        if link_name in v.get("library_names", []) and "windows" not in k
    ]
    if exact:
        return min(exact, key=len)

    suffix = [
        k for k, v in downloads.items()
        if any(link_name.endswith(n) for n in v.get("library_names", []))
        and "windows" not in k
    ]
    if suffix:
        return min(suffix, key=len)
    return None


def pretty_name(key: str) -> str:
    """downloads.py key -> stable component name.

    openssl-3.0 -> openssl, cpython-3.13 -> cpython, uuid -> libuuid.
    """
    if key == "uuid":
        return "libuuid"
    if key.startswith(("cpython-", "openssl-")):
        return key.split("-", 1)[0]
    return re.sub(r"-[\d.]+$", "", key)


# ---------------------------------------------------------------------------
# CycloneDX emission
# ---------------------------------------------------------------------------

def _licenses(entry: dict) -> list[dict]:
    if entry.get("license_public_domain"):
        return [{"license": {"name": "Public Domain"}}]
    return [{"license": {"id": spdx}} for spdx in entry.get("licenses", [])]


def make_component(key: str, entry: dict, linked_by: set[str], tag: str) -> dict:
    name = pretty_name(key)
    version = entry["version"]
    sha = entry["sha256"]
    url = entry["url"]
    purl = f"pkg:generic/{name}@{version}?download_url={url}&checksum=sha256:{sha}"
    hashes = [{"alg": "SHA-256", "content": sha}]
    _, ext = os.path.splitext(url)

    refs = [{"type": "distribution", "url": url, "hashes": hashes}]
    upstream = entry.get("upstream_url")
    if upstream and upstream != url:
        refs.append({
            "type": "distribution",
            "url": upstream,
            "comment": "upstream source (the URL above is a PBS mirror)",
        })
    refs.append({"type": "vcs", "url": PBS_REPO_URL})

    props = [
        {"name": "pbs:release", "value": tag},
        {"name": "pbs:download_key", "value": key},
    ]
    if linked_by:
        props.append({"name": "pbs:linked_by", "value": ",".join(sorted(linked_by))})

    component: dict = {
        "type": "library",
        "bom-ref": purl,
        "name": name,
        "version": version,
        "purl": purl,
        "hashes": hashes,
        "externalReferences": refs,
        "properties": props,
    }
    if mime := EXTENSION_TO_MIME.get(ext):
        component["mime-type"] = mime
    if licenses := _licenses(entry):
        component["licenses"] = licenses
    return component


def find_cpython_component(sbom: dict, parent_ref: str | None) -> dict:
    for comp in sbom.get("components", []):
        ref = comp.get("bom-ref", "")
        if parent_ref and ref == parent_ref:
            return comp
        if not parent_ref and ref.startswith("pkg:generic/cpython@") and "release=" in ref:
            return comp
    sys.exit("error: CPython component not found in SBOM (pass --parent-ref)")


def merge_into_sbom(sbom: dict, parent_ref: str, components: list[dict]) -> None:
    new_refs = {c["bom-ref"] for c in components}
    existing = sbom.setdefault("components", [])
    # Drop any prior pbs:* entries so this script is idempotent -- except the
    # parent cpython component, which legitimately carries pbs:* props after
    # enrich_parent_cpython().
    kept = [
        c for c in existing
        if c.get("bom-ref") not in new_refs
        and (
            c.get("bom-ref") == parent_ref
            or not any(p.get("name", "").startswith("pbs:") for p in c.get("properties", []))
        )
    ]
    sbom["components"] = kept + components

    deps = sbom.setdefault("dependencies", [])
    parent_dep = next((d for d in deps if d["ref"] == parent_ref), None)
    if parent_dep is None:
        parent_dep = {"ref": parent_ref, "dependsOn": []}
        deps.append(parent_dep)
    parent_dep["dependsOn"] = sorted(set(parent_dep.get("dependsOn", [])) | new_refs)


# ---------------------------------------------------------------------------
# Selection pipeline
# ---------------------------------------------------------------------------

def enrich_parent_cpython(parent: dict, entry: dict, tag: str) -> None:
    """Merge the cpython-X.Y downloads entry into the existing parent component.

    The parent represents the PBS binary distribution (already in the SBOM
    with the PBS tarball URL + SHA). We add the upstream python.org source
    tarball URL + SHA, the SPDX license, and bookkeeping properties -- rather
    than emit a second component that would collide on `name: cpython`.
    """
    sha = entry["sha256"]
    hashes = [{"alg": "SHA-256", "content": sha}]
    refs = parent.setdefault("externalReferences", [])
    if not any(r.get("url") == entry["url"] for r in refs):
        refs.append({
            "type": "distribution",
            "url": entry["url"],
            "comment": "upstream CPython source tarball (SHA-256 of source, not the PBS binary)",
            "hashes": hashes,
        })
    upstream = entry.get("upstream_url")
    if upstream and upstream != entry["url"] and not any(r.get("url") == upstream for r in refs):
        refs.append({
            "type": "distribution",
            "url": upstream,
            "comment": "upstream source (the URL above is a PBS mirror)",
        })
    if _licenses(entry) and "licenses" not in parent:
        parent["licenses"] = _licenses(entry)

    props = parent.setdefault("properties", [])
    for p in ({"name": "pbs:release", "value": tag},
              {"name": "pbs:download_key", "value": f"cpython-{entry['version'].rsplit('.', 1)[0]}"},
              {"name": "pbs:source_sha256", "value": sha}):
        if not any(x.get("name") == p["name"] for x in props):
            props.append(p)


def select(build_dir: Path, downloads: dict) -> tuple[dict[str, set[str]], list[str], dict, dict | None]:
    """Return {download_key: linked_by_set}, warnings, python_json, cpython_entry.

    The cpython entry is returned separately so main() can merge it into the
    existing parent component instead of emitting a second cpython component.
    """
    python_json = json.loads((build_dir / "PYTHON.json").read_text())
    selected: dict[str, set[str]] = {}
    warnings: list[str] = []

    for link, users in sorted(discover_linked_libs(python_json, build_dir).items()):
        key = resolve_download_key(link, downloads)
        if key is None:
            warnings.append(f"link {link!r} (used by {', '.join(sorted(users))}) has no downloads entry")
            continue
        name = pretty_name(key)
        if name in BUILD_TIME_ONLY:
            warnings.append(f"skipping {key!r} for link {link!r}: build-time only")
            continue
        if name not in KNOWN_RUNTIME:
            warnings.append(f"link {link!r} -> {key!r} ({name!r}) is not in the known-runtime allowlist; emitting anyway")
        selected.setdefault(key, set()).update(users)

    # CPython source: enriches the parent, not a separate component.
    cpython_key = f"cpython-{python_json['python_major_minor_version']}"
    cpython_entry: dict | None = downloads.get(cpython_key)
    if cpython_entry is None:
        warnings.append(f"no downloads entry {cpython_key!r}; parent component will not be enriched with upstream source info")
    elif cpython_entry["version"] != python_json["python_version"]:
        warnings.append(
            f"{cpython_key} downloads.version={cpython_entry['version']} != build.version={python_json['python_version']}"
        )
        cpython_entry = None

    # Tix ships as Tcl scripts, not a linked library; detect by filename.
    install_lib = build_dir / "install" / "lib"
    if "tix" in downloads and install_lib.is_dir() and any(
        p.name.lower().startswith("tix") for p in install_lib.iterdir()
    ):
        selected.setdefault("tix", set()).add("_tkinter")

    return selected, warnings, python_json, cpython_entry


def release_from_ref(ref: str) -> str | None:
    m = re.search(r"[?&]release=([^&]+)", ref)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--sbom", required=True, type=Path, help="input CycloneDX JSON (patched in place unless -o given)")
    ap.add_argument("--build", required=True, type=Path, help="unpacked PBS full archive (dir containing PYTHON.json)")
    ap.add_argument("--tag", help="PBS release tag (default: read from CPython component purl)")
    ap.add_argument("--downloads", type=Path, help="local downloads.json/py instead of fetching from GitHub")
    ap.add_argument("--parent-ref", help="bom-ref of the CPython component (default: auto-detect)")
    ap.add_argument("-o", "--output", type=Path, help="output path (default: overwrite --sbom)")
    ap.add_argument("--strict", action="store_true", help="exit non-zero on any warning")
    args = ap.parse_args()

    sbom = json.loads(args.sbom.read_text())
    parent = find_cpython_component(sbom, args.parent_ref)
    parent_ref = parent["bom-ref"]

    tag = args.tag or release_from_ref(parent_ref)
    if not tag:
        sys.exit("error: --tag not given and CPython bom-ref has no release= qualifier")

    if args.downloads:
        downloads = load_downloads(args.downloads.read_text(), args.downloads.name)
        source = str(args.downloads)
    else:
        downloads, source = fetch_downloads(tag)

    selected, warnings, python_json, cpython_entry = select(args.build, downloads)
    if python_json["python_version"] != parent.get("version"):
        warnings.append(
            f"build python={python_json['python_version']} but SBOM cpython component version={parent.get('version')}"
        )

    if cpython_entry is not None:
        enrich_parent_cpython(parent, cpython_entry, tag)

    components = [make_component(k, downloads[k], selected[k], tag) for k in sorted(selected)]
    merge_into_sbom(sbom, parent_ref, components)

    out = args.output or args.sbom
    out.write_text(json.dumps(sbom, indent=2, ensure_ascii=False) + "\n")

    print(f"downloads: {source}")
    print(f"parent:    {parent_ref}")
    print(f"{'key':16} {'version':14} name             linked by")
    for k in sorted(selected):
        e = downloads[k]
        print(f"{k:16} {e['version']:14} {pretty_name(k):16} {','.join(sorted(selected[k])) or '-'}")
    for w in warnings:
        print(f"warning: {w}", file=sys.stderr)
    print(f"wrote {len(components)} PBS components to {out}")
    if warnings and args.strict:
        sys.exit(1)


if __name__ == "__main__":
    main()
