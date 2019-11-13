#!/bin/bash
#
# $Id: columnstore_startup.sh 3705 2013-08-07 19:47:20Z dhill $
#
# columnstore_startup.sh steps for columnstore single server startup after install

quiet=0

columnstore-post-install
echo -e "1\n1\n1\n1\n1\n1\n" | postConfigure
if [ $? -eq 0 ]; then
	exit 0
else
	exit 1
fi
