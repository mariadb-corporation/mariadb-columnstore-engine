#!/usr/bin/env python3
"""Check that third-party code copied into this repository ("vendored") is
declared in build/vendored_code.json, and that the manifest agrees with
cmake/sbom_3rdparty.cmake and THIRD-PARTY-NOTICES.

Run from anywhere:  python3 build/check_vendored_code.py
Exit code 0 when everything is consistent, 1 otherwise.

Only vendored code that kept its license header can be found.
"""

import fnmatch
import json
import os
import re
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MANIFEST = "build/vendored_code.json"
SBOM_CMAKE = "cmake/sbom_3rdparty.cmake"
NOTICES = "THIRD-PARTY-NOTICES"

# Phrases that appear in the license headers of most third-party code.
# Matched case-insensitively, once per file.
PHRASES = [
    "Permission is hereby granted",
    "Redistributions of source code",
    "SPDX-License-Identifier",
    "All rights reserved",
    "Boost Software License",
    "Distributed under the",
]
PHRASE_RE = re.compile("|".join(re.escape(p) for p in PHRASES), re.IGNORECASE)

SBOM_CALL_RE = re.compile(r"columnstore_sbom_vendored\(\s*([A-Za-z0-9_.-]+)")

FIX_NEW_COMPONENT = """\
A file carries a third-party license header but is not in build/vendored_code.json.
For a new vendored component:
  1. add a columnstore_sbom_vendored(...) entry to cmake/sbom_3rdparty.cmake (if it has a version)
  2. add a section with the license text to THIRD-PARTY-NOTICES
  3. add a row to build/vendored_code.json
For our own code that happens to match a phrase: add the path to "ignore" in the manifest.
"""

FIX_MISMATCH = """\
Fix the mismatches above so that build/vendored_code.json, cmake/sbom_3rdparty.cmake
and THIRD-PARTY-NOTICES describe the same set of vendored components.
"""


def tracked_files():
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO_ROOT, check=True, capture_output=True
    ).stdout.decode()
    files = [f for f in out.split("\0") if f]
    # Submodule paths are listed by git but are directories in the tree.
    return [f for f in files if os.path.isfile(os.path.join(REPO_ROOT, f))]


def read_text(path):
    with open(os.path.join(REPO_ROOT, path), encoding="utf-8", errors="ignore") as f:
        return f.read()


def load_manifest():
    with open(os.path.join(REPO_ROOT, MANIFEST), encoding="utf-8") as f:
        manifest = json.load(f)
    components = manifest.get("components", [])
    ignore = manifest.get("ignore", [])
    for c in components:
        for key in ("name", "files", "license", "sbom", "notice"):
            if key not in c:
                sys.exit("%s: component %r lacks the %r field" % (MANIFEST, c.get("name"), key))
    return components, ignore


def strip_parentheses(text):
    """Remove (possibly nested) parenthesised text, e.g. '(function in_cksum)'."""
    out, depth = [], 0
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(depth - 1, 0)
        elif depth == 0:
            out.append(ch)
    return "".join(out)


def notices_files():
    """Paths named on the 'Files:' lines (with continuation lines) of the notices."""
    paths = set()
    lines = read_text(NOTICES).splitlines()
    i = 0
    while i < len(lines):
        if lines[i].startswith("Files:"):
            text = lines[i][len("Files:"):]
            i += 1
            while i < len(lines) and lines[i].startswith((" ", "\t")):
                text += " " + lines[i]
                i += 1
            for token in strip_parentheses(text).replace(",", " ").split():
                if "/" in token:
                    paths.add(token)
        else:
            i += 1
    return paths


def main():
    problems = []
    files = tracked_files()
    file_set = set(files)
    components, ignore = load_manifest()

    # 1. license-phrase scan
    component_files = {f: c["name"] for c in components for f in c["files"]}
    unknown_hits = []
    hits = 0
    for path in files:
        if not PHRASE_RE.search(read_text(path)):
            continue
        hits += 1
        if path in component_files:
            continue
        if any(fnmatch.fnmatch(path, pattern) for pattern in ignore):
            continue
        unknown_hits.append(path)
    for path in unknown_hits:
        problems.append("%s: license header found, file not in %s" % (path, MANIFEST))

    # 2. manifest files exist; ignore entries still match something
    for path, name in component_files.items():
        if path not in file_set:
            problems.append("%s: file %s of component %r is not in the tree" % (MANIFEST, path, name))
    for pattern in ignore:
        if not any(fnmatch.fnmatch(path, pattern) for path in files):
            problems.append("%s: ignore entry %r matches no tracked file" % (MANIFEST, pattern))

    # 3. sbom rows vs cmake/sbom_3rdparty.cmake
    sbom_names = {c["name"] for c in components if c["sbom"]}
    cmake_names = set(SBOM_CALL_RE.findall(read_text(SBOM_CMAKE)))
    for name in sorted(sbom_names - cmake_names):
        problems.append("%r: sbom=true in %s but no columnstore_sbom_vendored() call in %s" % (name, MANIFEST, SBOM_CMAKE))
    for name in sorted(cmake_names - sbom_names):
        problems.append("%r: declared in %s but no row with sbom=true in %s" % (name, SBOM_CMAKE, MANIFEST))

    # 4. notice rows vs THIRD-PARTY-NOTICES "Files:" lines
    notice_files = {f: c["name"] for c in components if c["notice"] for f in c["files"]}
    in_notices = notices_files()
    for path in sorted(set(notice_files) - in_notices):
        problems.append("%s: file of component %r (notice=true) is not on any 'Files:' line of %s" % (path, notice_files[path], NOTICES))
    for path in sorted(in_notices - set(notice_files)):
        problems.append("%s: listed on a 'Files:' line of %s but no row with notice=true in %s" % (path, NOTICES, MANIFEST))

    if problems:
        for p in problems:
            print("ERROR: " + p)
        print()
        print(FIX_NEW_COMPONENT if unknown_hits else FIX_MISMATCH)
        return 1

    print(
        "vendored code check: %d tracked files scanned, %d with license phrases, "
        "%d manifest components, %d ignore entries; all consistent."
        % (len(files), hits, len(components), len(ignore))
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
