How to use the ColumnStore UDF SDK

Obtain the MariaDB columnstore source code from https://github.com/mariadb-corporation/mariadb-columnstore-server
and follow the pre-requisite and build instructions.

Go into the utils/udfsdk directory.

At this point you can use the MCS_add() function template in udfsdk.cpp and udfmysql.cpp
files to create your own function or just try that function as is.
- Make the library
    $ make
- Copy the libudf_mysql.so.1.0.0 and libudfsdk.so.1.0.0 file to /usr/local/mariadb/columnstore/lib on
  every columnstore node.
    $ cp libudf_mysql.so.1.0.0 libudfsdk.so.1.0.0 /usr/local/mariadb/columnstore/lib/
- Restart ColumnStore
    $ mcsadmin restartsystem y
- Using the mariadb client add the user defined function, e.g,
    $ mariadb
    > create function mcs_add returns integer soname 'libudf_mysql.so';
    > create function mcs_isnull returns string soname 'libudf_mysql.so';

You should now be able to use the mcs_add(arg1, arg2) and mcs_isnull(arg) functions in the select and/or where clauses
of SQL statements.
