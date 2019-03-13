API Reference
=============

The UDAF API consists of a set of classes and structures used to implement a MariaDB Columnstore UDAF and/or UDAnF.

The classes and structures are:

* class UDAFMap
* struct UserData
* class mcsv1Context
* struct ColumnDatum
* class mcsv1_UDAF

The following Columnstore classes are also used:

* execplan::CalpontSystemCatalog::ColDataType
* messageqcpp::ByteStream

We also define COL_TYPES as a vector of pairs containing the column name and it's type.

