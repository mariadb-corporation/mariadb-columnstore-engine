# Runs the sql files in the working_dml directory.
# Dependencies:
# 1) A database named dml w/ a datatypetestm table populated via the script in genii/mysql/scripts/create_datatypetestm.sql.
# 2) A database named dml w/ a datatypetestm table populated via the script in genii/mysql/scripts/create_datatypetestm.sql in the reference
#    database.  Table must be a MyISAM table and have been altered so that the string columns are all binary.
# 3) 1 GB tpch orders and lineitem tables populated in the two databases.

./queryTester -q working_dml -u dml -g -d dml -r Tr
