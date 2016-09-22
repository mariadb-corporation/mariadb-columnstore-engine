rpmmode=upgrade
if  [ "$1" -eq "$1" 2> /dev/null ]; then
	if [ $1 -ne 1 ]; then
		rpmmode=erase
	fi
else
	rpmmode=erase
fi

if [ $rpmmode = erase ]; then
	test -x /usr/local/mariadb/columnstore/bin/pre-uninstall && /usr/local/mariadb/columnstore/bin/pre-uninstall
fi

exit 0

