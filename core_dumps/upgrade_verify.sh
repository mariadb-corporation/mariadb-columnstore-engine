#!/usr/bin/env bash

set -xeuo pipefail

SCHEMA_DIR=$(dirname "$0")
NAME1='airports'
NAME2='airlines'


test_data ()
{
    NAME=$1
    mariadb --init-command="SET sql_mode=''" -vvv -e "select * into outfile '/tmp/${NAME}.test.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM columnstore_bts.${NAME};"
    diff <(tail -n +2 "${NAME}.csv") <(tail -n +2  "/tmp/${NAME}.test.csv")
    rm "/tmp/${NAME}.test.csv"
}


test_data "$NAME1"
test_data "$NAME2"
