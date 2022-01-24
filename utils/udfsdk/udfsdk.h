/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (c) 2019 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/***********************************************************************
 *   $Id$
 *
 *
 ***********************************************************************/

/**
 * MariaDB ColumnStore interface for writing a user defined function (UDF).
 *
 * The basic steps are:
 *
 * 1. add the UDF function interface in udfsdk.h
 * 2. add the UDF function implementation in udfsdk.cpp
 * 3. add the connector stub for this UDF function in udfsdk.cpp
 * 4. build the dynamic library libudfsdk
 * 5. put the library in /usr/local/mariadb/columnstore/lib of all modules
 * 6. restart MariaDB ColumnStore
 * 7. Register the new functions with the commands like:
 *
 *    CREATE FUNCTION mcs_add returns REAL soname 'libudfsdk.so';
 *    CREATE FUNCTION mcs_isnull returns BOOL soname 'libudfsdk.so';
 *
 * The UDF functions run distributedly in the ColumnStore engine. The evaluation
 * is row by row. Aggregate UDF is currently not supported. Two examples are
 * given in this file to demonstrate the steps that it takes to create a UDF
 * function. More examples can be found in utils/funcexp/func_*.cpp.
 */

#include "funcexp.h"
#include "functor.h"

#if defined(_MSC_VER) && defined(UDFSDK_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace udfsdk
{
/**
 * UDFSDK interface. Do not make modification here.
 */
class UDFSDK
{
 public:
  EXPORT UDFSDK();

  EXPORT ~UDFSDK();

  EXPORT funcexp::FuncMap UDFMap() const;

 protected:
 private:
  // defaults okay
  // UDFSDK(const UDFSDK& rhs);
  // UDFSDK& operator=(const UDFSDK& rhs);
};

/**
 * Example: MCS_add (args1, args2)
 *
 * MCS_add takes two arguments of any data type. It returns a double result.
 *
 * The function interface is defined here. All UDF functions are derived from
 * class funcexp::Func. A set of getXXXval interface APIs are declared in the
 * parent class Func, which will be called by IDB function and expression (F&E)
 * framwork when evaluating the function. Which API to be called depends on
 * the context of the function in the SQL query, i.e., the result type that
 * the function is expected to return.
 *
 * For example, given the following two queries, different APIs will be called
 * to evaluate the function MCS_add.
 *
 * select MCS_add(int1, int2) from t1;
 * getDoubleVal() is called, because the result type of MCS_add is DOUBLE(real).
 *
 * select substr(string1, int1, MCS_add(int1+int2));
 * getIntVal() will be called, because MCS_add() is passed as the third argument
 * to substr function, and an integer result is expected.
 *
 * If one API is not implemented but called for a function, MCS-5001 error will
 * be returned.
 */
class MCS_add : public funcexp::Func
{
 public:
  /*
   * Constructor. Pass the function name to the base constructor.
   */
  MCS_add() : Func("mcs_add")
  {
  }

  /*
   * Destructor. MCS_add does not need to do anything here to clean up.
   */
  virtual ~MCS_add()
  {
  }

  /**
   * Decide on the function's operation type
   *
   * Operation type decides which API needs to be called for each function
   * parameter. Sometimes it is obvious. e.g. for function substr (c1, c2, c3),
   * one knows that getStrVal(), getIntVal() and getIntVal() should be called for
   * the three parameters in sequence. In that case, a dummy type can be returned
   * because it won't be used in the function implementation. Sometimes the
   * operation type is decided by the data type of the function parameters.
   * e.g., isnull(c1) function, one should call the corresponding getXXXval()
   * function that in compatible with the result type of c1.
   *
   * @parm fp vector of function parameters
   *       Each element is a boost::shared_ptr of execplan::ParseTree. class
   *       ParseTree is defined in ~/dbcon/execplan/parsetree.h
   * @parm resultType result type of this function
   *       Sometimes it may affect the operation type, but most of the time it
   *       can be ignored. Struct ColType is defined in ~/dbcon/execplan/calpontsystemcatalog.h
   * @return operation type for this function
   *
   * This function is called only one from the connector. Once it's determined, it
   * will be passed to the getXXXval() APIs during function evaluation.
   */
  execplan::CalpontSystemCatalog::ColType operationType(funcexp::FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  /**
   * Returns an integer result of this function.
   * All the getXXXvalue APIs take the same arguments. They will be called
   * for every row in the result set when the function is being evaluated.
   * So these functions needs to be efficient.
   *
   * @parm row reference of the current row
   * @parm fp function parameters
   * @parm isNull NULL indicator throughout this function evaluation.
   *       the same reference is passed to all the function argument
   *       evaluations. One always need to know if any argument is NULL
   *       to decide the result of the function. It's explained in detail
   *       in MCS_isnull() function example.
   * @parm op_ct the operation type that is determined in operationType().
   *
   */
  virtual int64_t getIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns a double result of this function.
   */

  virtual double getDoubleVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                              execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns a long double result of this function.
   */

  virtual long double getLongDoubleVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                       execplan::CalpontSystemCatalog::ColType& op_ct);
  /**
   * Returns a float result of this function.
   */
  virtual float getFloatVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns a string result of this function.
   */
  virtual std::string getStrVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns a bool result of this function.
   */
  virtual bool getBoolVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                          execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns a decimal result of this function.
   *
   * IDB_Decimal is defined in ~/execplan/treenode.h
   */
  virtual execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns an integer representation of a date result of the function.
   *
   * Check the date/time functions in ~/utils/funcexp for implementation
   * example of this API.
   */
  virtual int32_t getDateIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns an integer representation of a datetime result of the function.
   *
   * Check the date/time functions in ~/utils/funcexp for implementation
   * example of this API.
   */
  virtual int64_t getDatetimeIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/**
 * Example: MCS_isnull(arg1)
 *
 * The purpose of this example is to demostrate the NULL handling in the UDF interface
 */
class MCS_isnull : public funcexp::Func
{
 public:
  /*
   * Constructor. Pass the function name to the base constructor.
   */
  MCS_isnull() : Func("mcs_isnull")
  {
  }

  /*
   * Destructor. MCS_add does not need to do anything here to clean up.
   */
  virtual ~MCS_isnull()
  {
  }

  /**
   * Decide on the function's operation type
   */
  execplan::CalpontSystemCatalog::ColType operationType(funcexp::FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  /**
   * Returns an integer result of this function.
   */
  virtual int64_t getIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns a double result of this function.
   */
  virtual double getDoubleVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                              execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns a double result of this function.
   */
  virtual long double getLongDoubleVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                       execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns a float result of this function.
   */
  virtual float getFloatVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns a string result of this function.
   */
  virtual std::string getStrVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns a bool result of this function.
   */
  virtual bool getBoolVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                          execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns a decimal result of this function.
   *
   * IDB_Decimal is defined in ~/execplan/treenode.h
   */
  virtual execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns an integer representation of a date result of the function.
   *
   * Check the date/time functions in ~/utils/funcexp for implementation
   * example of this API.
   */
  virtual int32_t getDateIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                execplan::CalpontSystemCatalog::ColType& op_ct);

  /**
   * Returns an integer representation of a datetime result of the function.
   *
   * Check the date/time functions in ~/utils/funcexp for implementation
   * example of this API.
   */
  virtual int64_t getDatetimeIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

}  // namespace udfsdk

#undef EXPORT

// vim:ts=4 sw=4:
