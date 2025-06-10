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
  json_engine_t je;
  MEM_ROOT_DYNAMIC_ARRAY array;

#if MYSQL_VERSION_ID >= 120100
  int je_stack[JSON_DEPTH_LIMIT], buffer_array[JSON_DEPTH_LIMIT];

  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &je.stack, sizeof(int), &je_stack,
                              JSON_DEPTH_LIMIT, 0, MYF(0));
  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &array, sizeof(int), &buffer_array,
                              JSON_DEPTH_LIMIT, 0, MYF(0));
#endif

  const auto js_ns = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";
  const string_view js = js_ns.unsafeStringRef();

  using DynamicString = unique_ptr<DYNAMIC_STRING, decltype(&dynstr_free)>;

  DynamicString str{new DYNAMIC_STRING(), dynstr_free};
  if (init_dynamic_string(str.get(), NULL, 0, 0))
    goto error;

#if MYSQL_VERSION_ID >= 120100
  if (json_normalize(str.get(), js.data(), js.size(), getCharset(fp[0]), NULL, &je, &array))
#else
  if (json_normalize(str.get(), js.data(), js.size(), getCharset(fp[0])))
    goto error;
#endif

  return str->str;

error:
  isNull = true;
  return "";
}
}  // namespace funcexp
