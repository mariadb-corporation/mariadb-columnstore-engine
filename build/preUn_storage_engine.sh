
rpmmode=upgrade
if  [ "$1" -eq "$1" 2> /dev/null ]; then
	if [ $1 -ne 1 ]; then
		rpmmode=erase
	fi
else
	rpmmode=erase
fi

if [ $rpmmode = erase ]; then
	columnstore-pre-uninstall

	# Best-effort removal of ColumnStore SELinux policy on erase
	if command -v semodule >/dev/null 2>&1; then
	  if semodule -l 2>/dev/null | grep -q '^columnstore\b'; then
	    semodule -r columnstore || true
	  fi
	fi
fi

exit 0

