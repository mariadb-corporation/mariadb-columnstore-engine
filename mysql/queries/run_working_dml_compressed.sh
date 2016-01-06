# Runs the sql files in the working_dmlc directory.
# Dependencies:
# 1) A database named dmlc w/ a datatypetestm table populated via the script in genii/mysql/scripts/create_datatypetestm.sql.
# 2) A database named dmlc w/ a datatypetestm table populated via the script in genii/mysql/scripts/create_datatypetestm.sql in the reference
#    database.  Table must be a MyISAM table and have been altered so that the string columns are all binary.
# 3) 1 GB tpch orders and lineitem tables populated in the two databases.

#
# NOTE:  We skip the qa_functions folder here for two reasons:
#        1) It takes forever against compressed tables.
#	 2) We run the compressed run (test104) with UM joins (PmMaxMemorySmallSide = 1).  There are some updates with sub selects in qa_functions folder that will exceed the memory
#	    limit when run with PmMaxMemorySmallSide set to 1.
#

./queryTester -q working_dml -u dmlc -g -t misc -r Tr
./queryTester -q working_dml -u dmlc -g -t qa_sub -r Tr
