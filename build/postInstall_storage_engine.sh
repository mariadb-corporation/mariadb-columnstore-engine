echo "Start post install script."

rpmmode=install
if  [ "$1" -eq "$1" 2> /dev/null ]; then
	if [ $1 -ne 1 ]; then
		rpmmode=upgrade
	fi
fi

mkdir -p /var/lib/columnstore/local
columnstore-post-install --rpmmode=$rpmmode
