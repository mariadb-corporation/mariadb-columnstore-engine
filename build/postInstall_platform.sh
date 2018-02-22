rpmmode=install
if  [ "$1" -eq "$1" 2> /dev/null ]; then
	if [ $1 -ne 1 ]; then
		rpmmode=upgrade
	fi
fi

prefix=/usr/local

test -x /usr/local/mariadb/columnstore/bin/post-install && /usr/local/mariadb/columnstore/bin/post-install --prefix=$prefix --rpmmode=$rpmmode

