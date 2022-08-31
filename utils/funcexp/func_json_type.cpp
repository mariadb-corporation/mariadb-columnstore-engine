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
CalpontSystemCatalog::ColType Func_json_type::operationType(FunctionParm& fp,
                                                            CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_type::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                 execplan::CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  json_engine_t jsEg;
  string result;

  initJSEngine(jsEg, getCharset(fp[0]), js);

  if (json_read_value(&jsEg))
  {
    isNull = true;
    return "";
  }

  switch (jsEg.value_type)
  {
    case JSON_VALUE_OBJECT: result = "OBJECT"; break;
    case JSON_VALUE_ARRAY: result = "ARRAY"; break;
    case JSON_VALUE_STRING: result = "STRING"; break;
    case JSON_VALUE_NUMBER: result = (jsEg.num_flags & JSON_NUM_FRAC_PART) ? "DOUBLE" : "INTEGER"; break;
    case JSON_VALUE_TRUE:
    case JSON_VALUE_FALSE: result = "BOOLEAN"; break;
    default: result = "NULL"; break;
  }

  return result;
}
}  // namespace funcexp
