.. _cmakelists:

CMakeLists.txt
==============

For Columnstore 1.2, you compile your function by including it in the CMakeLists.txt file for the udfsdk.

You need only add the new .cpp files to the udfsdk_LIB_SRCS target list::

 set(udfsdk_LIB_SRCS udfsdk.cpp mcsv1_udaf.cpp allnull.cpp ssq.cpp median.cpp avg_mode.cpp)

If you create a new file for your MariaDB Aggregate function, add it to the target list for udf_mysql_LIB_SRCS::

 set(udf_mysql_LIB_SRCS udfmysql.cpp)

