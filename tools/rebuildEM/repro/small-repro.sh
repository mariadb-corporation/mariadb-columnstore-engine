#!/bin/bash
mariadb -e "DROP DATABASE IF EXISTS mcol_6321;
CREATE DATABASE mcol_6321;
USE mcol_6321;

CREATE TABLE t(x longtext, y int) ENGINE=Columnstore;

INSERT INTO t(x, y) SELECT CONCAT(seq, REPEAT('zuka',1000000)), seq FROM seq_1_to_840;
SELECT SUBSTR(x,1,10) FROM t;"

# Stop MCS.

echo "stopping MCS"
systemctl stop mariadb-columnstore

# Move system files with exception of oidbitmap.

echo "mkdir backup"
mkdir /var/lib/columnstore/data1/systemFiles/backup

echo "moving files into backup"
mv /var/lib/columnstore/data1/systemFiles/dbrm/* /var/lib/columnstore/data1/systemFiles/backup

echo "copying oidbitmap back"
cp -p /var/lib/columnstore/data1/systemFiles/backup/oidbitmap /var/lib/columnstore/data1/systemFiles/dbrm

# Rebuild EM

echo "rebuilding EM"
ASAN_OPTIONS='detect_leaks=0' mcsRebuildEM -v -y >/tmp/rebuild-log
chmod a+rw /var/lib/columnstore/data1/systemFiles/dbrm/BRM_saves_em

# Start MCS and verify that table is readable.

echo "starting MCS"
systemctl start mariadb-columnstore

mariadb -e "SELECT SUBSTR(x,1,10) FROM mcol_6321.t;"

# Stop MCS and restore files FROM backup.

echo "stopping MCS again to copy files back"
systemctl stop mariadb-columnstore

echo "deleting restored files"
rm -f /var/lib/columnstore/data1/systemFiles/dbrm/*

echo "list /dev/shm/*"
ls /dev/shm

echo "copy files back"
cp -p /var/lib/columnstore/data1/systemFiles/backup/* /var/lib/columnstore/data1/systemFiles/dbrm

echo "delete backup directory"
rm -rf /var/lib/columnstore/data1/systemFiles/backup

# Start MCS with old system files and verify table is readable.

echo "starting MCS"
systemctl start mariadb-columnstore

mariadb -e "SELECT SUBSTR(x,1,10) FROM mcol_6321.t;"

# Finish the test.

mariadb -e "DROP DATABASE mcol_6321;"
