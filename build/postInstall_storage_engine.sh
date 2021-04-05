rpmmode=install
if  [ "$1" -eq "$1" 2> /dev/null ]; then
	if [ $1 -ne 1 ]; then
		rpmmode=upgrade
	fi
fi

# Temporary install-time workaround for external pre-built TBB library
ln -s /usr/lib64/libtbb.so /usr/lib64/libtbb.so.12.1
ln -s /usr/lib64/libtbb.so /usr/lib64/libtbb.so.12

mkdir -p /var/lib/columnstore/local
columnstore-post-install --rpmmode=$rpmmode

