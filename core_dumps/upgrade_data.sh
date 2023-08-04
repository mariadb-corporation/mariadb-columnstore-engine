#!/usr/bin/env bash

set -xeuo pipefail

MARIADB=$(which mysql)
CPIMPORT=$(which cpimport)
SCHEMA_DIR=$(dirname "$0")
NAME1='airports'
NAME2='airlines'


get_data ()
{
  NAME=$1
  if curl -o "${SCHEMA_DIR}/${NAME}.csv" -# "https://sample-columnstore-data.s3.us-west-2.amazonaws.com/${NAME}.csv"; then
     echo -e "Downloaded '${NAME}.csv' ... done\n"
  else
     echo -e "Downloading '${NAME}.csv' ... failed"
     exit 1
  fi
}

import_data ()
{
    NAME=$1
    echo -e "\nLoading '${NAME}.csv' with cpimport ..."
    if ! $CPIMPORT -m 1 -s ',' -E '"' columnstore_bts "${NAME}" -l "${SCHEMA_DIR}/${NAME}.csv"; then
        echo -e "loading '${NAME}.csv' ... fail"
        exit 1
    fi
}


if $MARIADB <"${SCHEMA_DIR}"/columnstore_schema.sql &>/dev/null; then
    echo -e "Creating 'columnstore_bts' schema..." "done"
else
    echo -e "Creating 'columnstore_bts' schema..." "fail"
    exit 1
fi

get_data "$NAME1"
get_data "$NAME2"

import_data "$NAME1"
import_data "$NAME2"
