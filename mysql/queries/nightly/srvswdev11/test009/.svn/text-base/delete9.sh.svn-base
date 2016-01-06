#!/bin/bash
#
# Repeats deletes from test009 table.
# Parms:
#   DB
#     database to run against.
#   LOADs
#     Number of deletes to run.
#   ROWSPERLOAD
#     Number of rows in each batch.

if [ $# -lt 3 ]
then
        echo "delete9.sh db loads rowsPerLoad - requires three parms."
        exit
fi
DB=$1
LOADS=$2
ROWSPERLOAD=$3
ROLLBACK_ROWS=21123
TABLE=test009
DELETELOGTABLE=test009_delete

#
# Loop and delete.  First do a partial delete of the batch and roll it back, then really delete the batch.
#
rm -f delete.sql.log
rm -f stop.txt
for((i=1; i <= $LOADS; i++))
do
        echo "Deleting batch $i of $LOADS."

	#
	# Delete part of the batch, then rollback.
	#
        echo "set autocommit=0;" > delete.sql;
        echo "delete from test009 where batch = $i and c2 <= $ROLLBACK_ROWS;" >> delete.sql
        echo "rollback;" >> delete.sql
	$MYSQLCMD $DB -e "insert into $DELETELOGTABLE values ($i, 'delete w/ rollback batch $i', now(), null);"
        $MYSQLCMD $DB -vvv < delete.sql >> delete.sql.log 2>&1
	$MYSQLCMD $DB -e "update $DELETELOGTABLE set stop=now() where id=$i and what='delete w/ rollback batch $i';"

	#
	# Delete the batch and commit.
	#
        echo "delete from test009 where batch = $i;" > delete.sql
        echo "commit;" >> delete.sql
	$MYSQLCMD $DB -e "insert into $DELETELOGTABLE values ($i, 'delete batch $i', now(), null);"
        $MYSQLCMD $DB -vvv < delete.sql >> delete.sql.log 2>&1
	$MYSQLCMD $DB -e "update $DELETELOGTABLE set stop=now() where id=$i and what='delete batch $i';"

        if [ -f stop.txt ]; then
                echo "Found stop.txt.  Delete script stopping."
                exit
        fi
done

touch stop.txt
echo "Delete script complete."

