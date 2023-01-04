echo "Start mariadb-columnstore-engine/build/preUn_storage_engine.sh"
set -x

rpmmode=upgrade
if  [ "$1" -eq "$1" 2> /dev/null ]; then
	if [ $1 -ne 1 ]; then
		rpmmode=erase
	fi
else
	rpmmode=erase
fi

if [ $rpmmode = erase ]; then
	columnstore-pre-uninstall  1> /tmp/columnstore-pre-uninstall.log 2>&1
	cp /tmp/columnstore-pre-uninstall.log /var/log/mariadb/columnstore/.
fi

exit 0
