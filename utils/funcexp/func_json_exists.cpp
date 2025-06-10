#include "functor_json.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
#include "json_lib.h"
#include "rowgroup.h"
using namespace execplan;
using namespace rowgroup;

#include "dataconvert.h"

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_exists::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_exists::getBoolVal(Row& row, FunctionParm& fp, bool& isNull,
                                  CalpontSystemCatalog::ColType& type)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  int jsErr = 0;
  json_engine_t jsEg;

#if MYSQL_VERSION_ID >= 120100
  int jsEg_stack[JSON_DEPTH_LIMIT];
  json_path_step_t p_steps[JSON_DEPTH_LIMIT];

  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &jsEg.stack, sizeof(int), &jsEg_stack,
                              JSON_DEPTH_LIMIT, 0, MYF(0));

  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &path.p.steps, sizeof(json_path_step_t), &p_steps,
                              JSON_DEPTH_LIMIT, 0, MYF(0));
#endif

  initJSEngine(jsEg, getCharset(fp[0]), js);
  if (!path.parsed && parseJSPath(path, row, fp[1]))
    goto error;

  if (locateJSPath(jsEg, path, &jsErr))
  {
    if (jsErr)
      goto error;
    return false;
  }

  return true;

error:
  isNull = true;
  return false;
}
}  // namespace funcexp
