#include <string_view>
using namespace std;

#include "functor_json.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;
using namespace rowgroup;

#include "dataconvert.h"

#include "jsonhelpers.h"
using namespace funcexp::helpers;
namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_valid::operationType(FunctionParm& fp,
                                                             CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_valid::getBoolVal(Row& row, FunctionParm& fp, bool& isNull,
                                 CalpontSystemCatalog::ColType& /*type*/)
{
#if MYSQL_VERSION_ID >= 120100
  json_engine_t je;
  int je_stack[JSON_DEPTH_LIMIT];
  initJsonArray(&je.stack, sizeof(int), &je_stack);
#endif

  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

#if MYSQL_VERSION_ID >= 120100
  return json_valid(js.unsafeStringRef().data(), js.unsafeStringRef().size(), getCharset(fp[0]), &je);
#else
  return json_valid(js.unsafeStringRef().data(), js.unsafeStringRef().size(), getCharset(fp[0]));
#endif
}
}  // namespace funcexp
