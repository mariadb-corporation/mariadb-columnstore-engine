#!/bin/bash

DB=dmlc

$MYSQLCMD $DB -e "delete from test204;" > load.log 2>&1 
$CPIMPORTCMD $DB test204 test204.tbl >> load.log 2>&1 
