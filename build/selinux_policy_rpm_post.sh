#!/bin/sh
# Post-install script to load ColumnStore SELinux policy if SELinux is enabled
# This script must not introduce new runtime dependencies; it only uses coreutils and typical SELinux tools if present.

set -e

POLICY_PATH="/usr/share/columnstore/policy/selinux/columnstore.pp"

# If SELinux tooling is not present, or policy file missing, silently exit
command -v getenforce >/dev/null 2>&1 || exit 0
command -v semodule >/dev/null 2>&1 || exit 0

# Only attempt to install when SELinux is enforcing or permissive
MODE=$(getenforce 2>/dev/null || echo Disabled)
case "$MODE" in
  Enforcing|Permissive)
    if [ -r "$POLICY_PATH" ]; then
      # Install or upgrade the module; do not fail the entire package if this fails
      semodule -i "$POLICY_PATH" || true
    fi
    ;;
  *)
    # Disabled or unknown, do nothing
    :
    ;;
esac

exit 0
