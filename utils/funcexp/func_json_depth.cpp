#include "functor_json.h"
#include "functioncolumn.h"
#include "json_lib.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "dataconvert.h"
using namespace dataconvert;

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_depth::operationType(FunctionParm& fp,
                                                             CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

int64_t Func_json_depth::getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& op_ct)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return 0;

  int depth = 0, currDepth = 0;
  bool incDepth = true;
  json_engine_t jsEg;

#if MYSQL_VERSION_ID >= 120100
  int jsEg_stack[JSON_DEPTH_LIMIT];
  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &jsEg.stack, sizeof(int), &jsEg_stack,
                              JSON_DEPTH_LIMIT, 0, MYF(0));
#endif
  initJSEngine(jsEg, getCharset(fp[0]), js);

  do
  {
    switch (jsEg.state)
    {
      case JST_VALUE:
      case JST_KEY:
        if (incDepth)
        {
          currDepth++;
          incDepth = false;
          if (currDepth > depth)
            depth = currDepth;
        }
        break;
      case JST_OBJ_START:
      case JST_ARRAY_START: incDepth = true; break;
      case JST_OBJ_END:
      case JST_ARRAY_END:
        if (!incDepth)
          currDepth--;
        incDepth = false;
        break;
      default: break;
    }
  } while (json_scan_next(&jsEg) == 0);

  if (likely(!jsEg.s.error))
    return depth;

  isNull = true;
  return 0;
}
}  // namespace funcexp
