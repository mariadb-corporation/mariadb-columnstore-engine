#pragma once

#include "funcexp.h"
#include "functor.h"

namespace udfsdk
{

class bloom_and : public funcexp::Func
{
 public:
  bloom_and() : Func("bloom_and")
  {
  }

  ~bloom_and() override = default;

 
  execplan::CalpontSystemCatalog::ColType operationType(
      funcexp::FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;


  int64_t getIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;


  double getDoubleVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  float getFloatVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  bool getBoolVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int32_t getDateIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getDatetimeIntVal(rowgroup::Row& row, funcexp::FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

} // namespace udfsdk

