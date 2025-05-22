#include "functor_json.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
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
CalpontSystemCatalog::ColType Func_json_length::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

int64_t Func_json_length::getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                    execplan::CalpontSystemCatalog::ColType& /*op_ct*/)
{
  const auto& js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return 0;

  json_engine_t jsEg;
  int jsEg_stack[JSON_DEPTH_LIMIT];
  json_path_step_t p_steps[JSON_DEPTH_LIMIT];
  int length = 0;
  int err;

  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &jsEg.stack, sizeof(int), &jsEg_stack,
                              JSON_DEPTH_LIMIT, 0, MYF(0));
  initJSEngine(jsEg, getCharset(fp[0]), js);

  if (fp.size() > 1)
  {
    mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &path.p.steps, sizeof(json_path_step_t), &p_steps,
                              JSON_DEPTH_DEFAULT, 0, MYF(0));
    if (!path.parsed && parseJSPath(path, row, fp[1], false))
      goto error;

    if (locateJSPath(jsEg, path))
      goto error;
  }

  if (json_read_value(&jsEg))
    goto error;

  if (json_value_scalar(&jsEg))
    return 1;

  while (!(err = json_scan_next(&jsEg)) && jsEg.state != JST_OBJ_END && jsEg.state != JST_ARRAY_END)
  {
    switch (jsEg.state)
    {
      case JST_VALUE:
      case JST_KEY: length++; break;
      case JST_OBJ_START:
      case JST_ARRAY_START:
        if (json_skip_level(&jsEg))
          goto error;
        break;
      default: break;
    };
  }

  if (!err)
  {
    // Parse to the end of the JSON just to check it's valid.
    while (json_scan_next(&jsEg) == 0)
    {
    }
  }

  if (likely(!jsEg.s.error))
    return length;

error:
  isNull = true;
  return 0;
}
}  // namespace funcexp
