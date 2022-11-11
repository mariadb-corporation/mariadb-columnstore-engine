#include <string>
using namespace std;

#include "functor_json.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_array::operationType(FunctionParm& fp,
                                                             CalpontSystemCatalog::ColType& resultType)
{
  return fp.size() > 0 ? fp[0]->data()->resultType() : resultType;
}

string Func_json_array::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& type)
{
  if (fp.size() == 0)
    return "[]";

  const CHARSET_INFO* retCS = type.getCharset();
  string ret("[");

  if (appendJSValue(ret, retCS, row, fp[0]))
    goto error;

  for (size_t i = 1; i < fp.size(); i++)
  {
    ret.append(", ");
    if (appendJSValue(ret, retCS, row, fp[i]))
      goto error;
  }

  ret.append("]");
  return ret;

error:
  isNull = true;
  return "";
}

}  // namespace funcexp
