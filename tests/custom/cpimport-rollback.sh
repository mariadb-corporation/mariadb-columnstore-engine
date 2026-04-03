#!/bin/bash

echo "PATH: $PATH"

export PATH=$PATH:/usr/bin:/usr/sbin

DB_NAME="test_cp_rb"
TB_NAME="date_crash"

echo "1. Setting up test schema..."
mariadb -e "DROP DATABASE IF EXISTS ${DB_NAME};"
mariadb -e "CREATE DATABASE IF NOT EXISTS ${DB_NAME};"
mariadb -e "CREATE TABLE ${DB_NAME}.${TB_NAME} (id INT, dt DATE) ENGINE=ColumnStore;"

echo "2. Generating test data..."
seq 1 5000000 | awk '{print $1",2025-03-01"}' > /tmp/test_data0.csv
echo '100000000,\N' >>/tmp/test_data0.csv
seq 1 50000000 | awk '{print $1",2026-03-01"}' > /tmp/test_data.csv
echo '110000000,\N' >>/tmp/test_data.csv

echo "3.a. Loading data before the bigger bulk"
cpimport -s ',' ${DB_NAME} ${TB_NAME} /tmp/test_data0.csv
echo "3.b. Starting cpimport in background..."
cpimport -s ',' ${DB_NAME} ${TB_NAME} /tmp/test_data.csv &
CP_PID=$!

echo "4. Waiting 1 second for extents to allocate..."
sleep 1

echo "5. Sending SIGSEGV to force BulkRollbackMgr execution..."
kill -11 $CP_PID

wait $CP_PID

echo "6. Testing MAX(dt) - Will incorrectly return 0000-00-00:"
RESULT=`mariadb -N -e "SELECT MAX(dt) FROM ${DB_NAME}.${TB_NAME};"`

echo "MAX(dt): $RESULT"

mariadb -e "DROP DATABASE ${DB_NAME};" # cleanup

if [ "$RESULT" != "2026-03-01" ];
then
	exit 1
fi
