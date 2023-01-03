echo "Start  mariadb-columnstore-engine/build/postInstall_storage_engine.sh"
set -x
rpmmode=install
if  [ "$1" -eq "$1" 2> /dev/null ]; then
	if [ $1 -ne 1 ]; then
		rpmmode=upgrade
	fi
fi

mkdir -p /var/lib/columnstore/local
columnstore-post-install --rpmmode=$rpmmode 1> /tmp/columnstore-post-install.log 2>&1