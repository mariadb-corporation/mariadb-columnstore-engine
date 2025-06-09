#!/usr/bin/env python3

import argparse
import fnmatch
import sys

def read_patterns(filename):
    with open(filename, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def read_paths_with_comments(filename):
    paths = []
    with open(filename, 'r') as f:
        for line in f:
            raw = line.rstrip('\n')
            if not raw.strip():
                continue
            # Split into path and comment
            if '#' in raw:
                path, comment = raw.split('#', 1)
                path = path.strip()
                comment = comment.strip()
            else:
                path = raw.strip()
                comment = ''
            if path:
                paths.append((raw, path, comment))
    return paths

def print_red(msg):
    if sys.stdout.isatty():
        print("\033[31m" + msg + "\033[0m")
    else:
        print(msg)

def main():
    parser = argparse.ArgumentParser(description="Compare wildcard patterns to file paths.")
    parser.add_argument('patterns_file', help='File with wildcard patterns (one per line)')
    parser.add_argument('paths_file', help='File with file paths (one per line)')
    args = parser.parse_args()

    patterns = read_patterns(args.patterns_file)
    paths_with_comments = read_paths_with_comments(args.paths_file)

    # Track which paths are matched (by index)
    path_matched = [False] * len(paths_with_comments)

    # Collect patterns that don't match any path
    unmatched_patterns = []
    for pattern in patterns:
        matched = False
        for i, (raw, path, comment) in enumerate(paths_with_comments):
            if fnmatch.fnmatch(path, pattern):
                matched = True
                path_matched[i] = True
        if not matched:
            unmatched_patterns.append(pattern)

    if unmatched_patterns:
        print_red("The files declared in debian/mariadb-plugin-columnstore.install "
                  " are not added to CMakeLists via columnstore_* statements see cmake/ColumnstoreLibrary.cmake")
        for pattern in unmatched_patterns:
            print(f"- {pattern}")

    # Collect paths that weren't matched by any pattern
    unmatched_paths = []
    for (raw, path, comment), matched in zip(paths_with_comments, path_matched):
        if not matched:
            unmatched_paths.append((path, comment))

    if unmatched_paths:
        print_red("The files added via columnstore_* statements from cmake/ColumnstoreLibrary.cmake "
                  "are missing in debian/mariadb-plugin-columnstore.install file")
        for path, comment in unmatched_paths:
            if comment:
                print(f"- {path} {comment}")
            else:
                print(f"- {path}")

if __name__ == "__main__":
    main()
