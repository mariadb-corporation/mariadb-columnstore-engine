#!/bin/sh

#
# This script lists Columnstorelpont data files that do not have associated extent map entries.
#
# NOTES:  
# 1) Only looks in $COLUMNSTORE_INSTALL_DIR/data* for the data files.
# 2) Only checks for an existing extent with a matching OID, doesn't validate that there is an
#    existing extent for the exact segment.
#
# Close enough for hand grenades.

if [ -z "$COLUMNSTORE_INSTALL_DIR" ]; then
	COLUMNSTORE_INSTALL_DIR=/usr/local/mariadb/columnstore
fi

export COLUMNSTORE_INSTALL_DIR=$COLUMNSTORE_INSTALL_DIR

if [ $COLUMNSTORE_INSTALL_DIR != "/usr/local/mariadb/columnstore" ]; then
	export PATH=$COLUMNSTORE_INSTALL_DIR/bin:$COLUMNSTORE_INSTALL_DIR/mysql/bin:/bin:/usr/bin
	export LD_LIBRARY_PATH=$COLUMNSTORE_INSTALL_DIR/lib:$COLUMNSTORE_INSTALL_DIR/mysql/lib
fi

cd $COLUMNSTORE_INSTALL_DIR

last=-1
existsInExtentMap=0
count=0

for i in $COLUMNSTORE_INSTALL_DIR/data*/*/*/*/*/*/FILE*cdf; do
        let count++
        oid=`$COLUMNSTORE_INSTALL_DIR/bin/file2oid.pl $i`
	if [ $last -ne $oid ]; then
		last=$oid
        	existsInExtentMap=`$COLUMNSTORE_INSTALL_DIR/bin/editem -o $oid | wc -l`
	fi
        if [ $existsInExtentMap -le 0 ]; then
                echo "Missing oid $oid path $i"
        fi
done
