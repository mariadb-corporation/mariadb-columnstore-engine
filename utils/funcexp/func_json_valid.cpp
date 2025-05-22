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
  int je_stack[JSON_DEPTH_LIMIT];
  json_engine_t je;

  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &je.stack, sizeof(int), &je_stack,
                              JSON_DEPTH_DEFAULT, 0, MYF(0));

  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  return json_valid(js.unsafeStringRef().data(), js.unsafeStringRef().size(), getCharset(fp[0]), &je);
}
}  // namespace funcexp
