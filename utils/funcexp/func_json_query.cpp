#include "functor_json.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

namespace funcexp
{
bool JSONEgWrapper::checkAndGetComplexVal(string& ret, int* error)
{
  if (json_value_scalar(this))
  {
    /* We skip scalar values. */
    if (json_scan_next(this))
      *error = 1;
    return true;
  }

  const uchar* tmpValue = value;
  if (json_skip_level(this))
  {
    *error = 1;
    return true;
  }

  ret.append((const char*)value, s.c_str - tmpValue);
  return false;
}

CalpontSystemCatalog::ColType Func_json_query::operationType(FunctionParm& fp,
                                                             CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_query::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& type)
{
  string ret;
  isNull = JSONPathWrapper::extract(ret, row, fp[0], fp[1]);
  return isNull ? "" : ret;
}
}  // namespace funcexp
