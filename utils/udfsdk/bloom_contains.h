#pragma once

#include "funcexp.h"
#include "functor.h"

namespace udfsdk
{

class MCS_bloom_contains : public funcexp::Func
{
 public:
  /*
   * Constructor. Pass the function name to the base constructor.
   */
  MCS_bloom_contains() : Func("bloom_contains")
  {
  }

  /*
   * Destructor. MCS_add does not need to do anything here to clean up.
   */
  ~MCS_bloom_contains() override = default;

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
  execplan::CalpontSystemCatalog::ColType operationType(
      funcexp::FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

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
  int64_t getIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  /**
   * Returns a double result of this function.
   */

  double getDoubleVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  /**
   * Returns a long double result of this function.
   */

  long double getLongDoubleVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;
  /**
   * Returns a float result of this function.
   */
  float getFloatVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  /**
   * Returns a string result of this function.
   */
  std::string getStrVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  /**
   * Returns a bool result of this function.
   */
  bool getBoolVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;

  /**
   * Returns a decimal result of this function.
   *
   * IDB_Decimal is defined in ~/execplan/treenode.h
   */
  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  /**
   * Returns an integer representation of a date result of the function.
   *
   * Check the date/time functions in ~/utils/funcexp for implementation
   * example of this API.
   */
  int32_t getDateIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  /**
   * Returns an integer representation of a datetime result of the function.
   *
   * Check the date/time functions in ~/utils/funcexp for implementation
   * example of this API.
   */
  int64_t getDatetimeIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

} // namespace udfsdk

