#include <string_view>
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
CalpontSystemCatalog::ColType Func_json_normalize::operationType(FunctionParm& fp,
                                                                 CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_normalize::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  using DynamicString = unique_ptr<DYNAMIC_STRING, decltype(&dynstr_free)>;

  DynamicString str{new DYNAMIC_STRING(), dynstr_free};
  if (init_dynamic_string(str.get(), NULL, 0, 0))
    goto error;

  if (json_normalize(str.get(), js.data(), js.size(), getCharset(fp[0])))
    goto error;

  return str->str;

error:
  isNull = true;
  return "";
}
}  // namespace funcexp
