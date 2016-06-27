#!/bin/sh
export LD_LIBRARY_PATH=/usr/local/mariadb/columnstore/lib:$LD_LIBRARY_PATH
export CALPONT_CONFIG_FILE=/usr/local/mariadb/columnstore/etc/Columnstore.xml
export PATH=$PATH:/usr/local/hadoop-0.20.2/bin:/usr/local/mariadb/columnstore/bin
export CALPONT_HOME=/usr/local/mariadb/columnstore/etc
hadoop dfs -cat $1 | cpimport $2 $3

