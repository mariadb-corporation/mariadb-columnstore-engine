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
	columnstore-pre-uninstall > columnstore-pre-uninstall.log
fi

exit 0
