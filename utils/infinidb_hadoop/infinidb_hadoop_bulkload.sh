#!/bin/sh
export LD_LIBRARY_PATH=/usr/local/MariaDB/Columnstore/lib:$LD_LIBRARY_PATH
export CALPONT_CONFIG_FILE=/usr/local/MariaDB/Columnstore/etc/Calpont.xml
export PATH=$PATH:/usr/local/hadoop-0.20.2/bin:/usr/local/MariaDB/Columnstore/bin
export CALPONT_HOME=/usr/local/MariaDB/Columnstore/etc
hadoop dfs -cat $1 | cpimport $2 $3

