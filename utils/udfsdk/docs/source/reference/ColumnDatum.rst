.. _ColumnDatum:

ColumnDatum
===========

Since aggregate functions can operate on any data type, we use the ColumnDatum struct to define the input row data. To be type insensiteve, data is stored in type static_any::any.

To access the data it must be type cast to the correct type using static_any::any::cast().

Example for int data:

::

   if (valIn.compatible(intTypeId)
     int myint = valIn.cast<int>();


For multi-paramter aggregations (not available in Columnstore 1.1), the colsIn array of next_value() contains the ordered set of row parameters.

For char, varchar, text, varbinary and blob types, columnData will be std::string.

The intTypeId above is one of a set of "static const static_any::any&" provided in mcsv1_UDAF for your use to figure out which type of data was actually sent. See the MEDIAN example for how these might be used to accept any numeric data type.

The provided values are:

* charTypeId
* scharTypeId
* shortTypeId
* intTypeId
* longTypeId
* llTypeId
* ucharTypeId
* ushortTypeId
* uintTypeId
* ulongTypeId
* ullTypeId
* floatTypeId
* doubleTypeId
* strTypeId

.. rubric:: The ColumnDatum members.

.. c:member:: CalpontSystemCatalog::ColDataType dataType;   

 ColDataType is defined in calpontsystemcatalog.h and can be any of the following:

.. _coldatatype:

.. list-table:: ColDataType
   :widths: 10 50
   :header-rows: 1

   * - Data Type
     - Usage
   * - BIT
     - Represents a binary 1 or 0. Stored in a single byte.
   * - TINYINT
     - A signed one byte integer
   * - CHAR
     - A signed one byte integer or an ascii char
   * - SMALLINT
     - A signed two byte integer
   * - DECIMAL
     - A Columnstore Decimal value. This is stored in the smallest integer type field that will hold the required precision.
   * - MEDINT
     - A signed four byte integer
   * - INT
     - A signed four byte integer
   * - FLOAT
     - A floating point number. Represented as a C++ float type.
   * - DATE
     - A Columnstore date stored as a four byte unsigned integer.
   * - BIGINT
     - A signed eight byte integer
   * - DOUBLE
     - A floating point number. Represented as a C++ double type.
   * - DATETIME
     - A Columnstore date-time stored as an eight byte unsigned integer.
   * - TIME
     - A Columnstore time stored as an eight byte unsigned integer.
   * - VARCHAR
     - A mariadb variable length string. Represented a std::string
   * - VARBINARY
     - A mariadb variable length binary. Represented a std::string that may contain embedded '0's
   * - CLOB
     - Has not been verified for use in UDAF
   * - BLOB
     - Has not been verified for use in UDAF
   * - UTINYINT
     - An unsigned one byte integer
   * - USMALLINT
     - An unsigned two byte integer
   * - UDECIMAL
     - DECIMAL, but no negative values allowed.
   * - UMEDINT
     - An unsigned four byte integer
   * - UINT
     - An unsigned four byte integer
   * - UFLOAT
     - FLOAT, but no negative values allowed.
   * - UBIGINT
     - An unsigned eight byte integer
   * - UDOUBLE
     - DOUBLE, but no negative values allowed.
   * - TEXT
     - Has not been verified for use in UDAF

.. c:member:: static_any::any  columnData;

 Holds the value for this column in this row.

.. c:member:: uint32_t    scale;

 If dataType is a DECIMAL type

.. c:member:: uint32_t    precision; 

 If dataType is a DECIMAL type

.. c:function:: ColumnDatum()

 Sets defaults.


