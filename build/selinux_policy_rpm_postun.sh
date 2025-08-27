#!/bin/sh
# Post-uninstall script to remove ColumnStore SELinux policy module if present
# No new runtime dependencies; use SELinux tools only if available.

set -e

# If SELinux tooling is not present, silently exit
command -v semodule >/dev/null 2>&1 || exit 0

# Remove the module if it is installed; do not fail package removal if this fails
if semodule -l 2>/dev/null | grep -q '^columnstore\b'; then
  semodule -r columnstore || true
fi

exit 0
