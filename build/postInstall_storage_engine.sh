rpmmode=install
if  [ "$1" -eq "$1" 2> /dev/null ]; then
	if [ $1 -ne 1 ]; then
		rpmmode=upgrade
	fi
fi

mkdir -p /var/lib/columnstore/local
columnstore-post-install --rpmmode=$rpmmode

# Attempt to load ColumnStore SELinux policy (best-effort, no hard dependency)
POLICY_PATH="/usr/share/columnstore/policy/selinux/columnstore.pp"
if command -v getenforce >/dev/null 2>&1 && command -v semodule >/dev/null 2>&1; then
  MODE=$(getenforce 2>/dev/null || echo Disabled)
  case "$MODE" in
    Enforcing|Permissive)
      if [ -r "$POLICY_PATH" ]; then
        semodule -i "$POLICY_PATH" || true
      fi
      ;;
    *)
      :
      ;;
  esac
fi

