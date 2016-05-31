#!/bin/bash
#
#=========================================================================================
mount /dev/sdj1 /mnt/qadbs
#
rm -rf /usr/local/mariadb/columnstore/data1/000.dir
rm -rf /usr/local/mariadb/columnstore/data2/000.dir
rm -rf /usr/local/mariadb/columnstore/data3/000.dir
rm -rf /usr/local/mariadb/columnstore/data4/000.dir
rm -f /usr/local/mariadb/columnstore/data1/systemFiles/dbrm/*
#
cp /mnt/qadbs/tpch/1m-1-10-100/dbrm/* /usr/local/mariadb/columnstore/data1/systemFiles/dbrm
#
cp -r /mnt/qadbs/tpch/1m-1-10-100/data1/000.dir /usr/local/mariadb/columnstore/data1 &
cp -r /mnt/qadbs/tpch/1m-1-10-100/data2/000.dir /usr/local/mariadb/columnstore/data2 &
cp -r /mnt/qadbs/tpch/1m-1-10-100/data3/000.dir /usr/local/mariadb/columnstore/data3 &
cp -r /mnt/qadbs/tpch/1m-1-10-100/data4/000.dir /usr/local/mariadb/columnstore/data4 &
#

