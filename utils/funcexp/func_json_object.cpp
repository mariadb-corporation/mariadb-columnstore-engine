#include <string_view>
using namespace std;

#include "functor_json.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "mcs_datatype.h"
using namespace datatypes;

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_object::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return fp.size() > 0 ? fp[0]->data()->resultType() : resultType;
}

string Func_json_object::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& type)
{
  if (fp.size() == 0)
    return "{}";

  const CHARSET_INFO* retCS = type.getCharset();
  string ret("{");

  if (appendJSKeyName(ret, retCS, row, fp[0]) || appendJSValue(ret, retCS, row, fp[1]))
    goto error;

  for (size_t i = 2; i < fp.size(); i += 2)
  {
    ret.append(", ");
    if (appendJSKeyName(ret, retCS, row, fp[i]) || appendJSValue(ret, retCS, row, fp[i + 1]))
      goto error;
  }

  ret.append("}");
  return ret;

error:
  isNull = true;
  return "";
}
}  // namespace funcexp
