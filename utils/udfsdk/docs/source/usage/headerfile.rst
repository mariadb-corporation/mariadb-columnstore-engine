.. _header_file:

Header file
===========

Usually, each UDA(n)F function will have one .h and one .cpp file plus code for the mariadb UDAF plugin which may or may not be in a separate file. It is acceptable to put a set of related functions in the same files or use separate files for each.

The easiest way to create these files is to copy them from an example closest to the type of function you intend to create.

Your header file must have a class defined that will implement your function. This class must be derived from mcsv1_UDAF and be in the mcsv1sdk namespace. The following examples use the "allnull" UDAF.

First you must include at least the following::

 #include "mcsv1_udaf.h"
 #include "calpontsystemcatalog.h"

Other headers as needed. Then::

 namespace mcsv1sdk
 {

Next you must create your class that will implement the UDAF. You must have a constructor, virtual destructor and all the methods that are declared pure in the base class mcsv1_UDAF. There are also methods that have base class implementations. These you may extended as needed. Other methods may be added if you feel a need for helpers. 

allnull uses the Simple Data Model. See :ref:`complexdatamodel` to see how that is used. See the MEDIAN example to see the dropValue() usage::

 class allnull : public  mcsv1_UDAF
 {
 public:
    allnull() : mcsv1_UDAF(){};
    virtual ~allnull(){};

    virtual ReturnCode init(mcsv1Context* context, ColumnDatum* colTypes);
    virtual ReturnCode reset(mcsv1Context* context);
    virtual ReturnCode nextValue(mcsv1Context* context, ColumnDatum* valsIn);
    virtual ReturnCode subEvaluate(mcsv1Context* context, const UserData* userDataIn);
    virtual ReturnCode evaluate(mcsv1Context* context, static_any::any& valOut);
 };










