mcsv1_udaf Introduction
=======================

mcsv1_udaf is a C++ API for writing User Defined Aggregate Functions (UDAF) and User Defined Analytic Functions (UDAnF) for the MariaDB Columstore engine. 

In Columnstore 1.1.0, functions written using this API must be compiled into the udfsdk and udf_mysql libraries of the Columnstore code branch.

The API has a number of features. The general theme is, there is a class that represents the function, there is a context under which the function operates, and there is a data store for intermediate values.

The steps required to create a function are:

* Decide on memory allocation scheme.
* Create a header file for your function.
* Create a source file for your function.
* Implement mariadb udaf api code.
* Add the function to UDAFMap in mcsv1_udaf.cpp
* Add the function to CMakeLists.txt in ./utils/udfsdk
* Compile udfsdk.
* Copy the compiled libraries to the working directories.

In 1.1.0, Columnstore does not have a plugin framework, so the functions have to be compiled into the libraries that Columnstore already loads.

