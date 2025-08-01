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
CalpontSystemCatalog::ColType Func_json_normalize::operationType(
    FunctionParm& fp, CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_normalize::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                           execplan::CalpontSystemCatalog::ColType& /*type*/)
{
#if MYSQL_VERSION_ID >= 120100
  json_engine_t je;
  MEM_ROOT_DYNAMIC_ARRAY array;
  int je_stack[JSON_DEPTH_LIMIT];
  struct json_norm_value *buffer_array[JSON_DEPTH_LIMIT];

  initJsonArray(NULL, &je.stack, sizeof(int), &je_stack);
  initJsonArray(NULL, &array, sizeof(struct json_norm_value*), buffer_array);
#endif

  const auto js_ns = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";
  const std::string_view js = js_ns.unsafeStringRef();

  using DynamicString = unique_ptr<DYNAMIC_STRING, decltype(&dynstr_free)>;

  DynamicString str{new DYNAMIC_STRING(), dynstr_free};
  if (init_dynamic_string(str.get(), NULL, 0, 0))
    goto error;

#if MYSQL_VERSION_ID >= 120100
  if (json_normalize(str.get(), js.data(), js.size(), getCharset(fp[0]), NULL, &je, &array))
#else
  if (json_normalize(str.get(), js.data(), js.size(), getCharset(fp[0])))
#endif
    goto error;

  return str->str;

error:
  isNull = true;
  return "";
}
}  // namespace funcexp
