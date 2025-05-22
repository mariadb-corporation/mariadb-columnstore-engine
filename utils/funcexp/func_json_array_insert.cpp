#include "functor_json.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
#include "json_lib.h"
#include "my_sys.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_array_insert::operationType(
    FunctionParm& fp, CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

string Func_json_array_insert::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                         execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto& js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  const CHARSET_INFO* cs = getCharset(fp[0]);

  json_engine_t jsEg;
  int jsEg_stack[JSON_DEPTH_LIMIT];
  string retJS;
  retJS.reserve(js.length() + 8);
  json_path_step_t p_steps[JSON_DEPTH_LIMIT];

  initJSPaths(paths, fp, 1, 2);

  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &jsEg.stack, sizeof(int), &jsEg_stack,
                              JSON_DEPTH_LIMIT, 0, MYF(0));

  utils::NullString tmpJS(js);
  for (size_t i = 1, j = 0; i < fp.size(); i += 2, j++)
  {
    const char* rawJS = tmpJS.str();
    const size_t jsLen = tmpJS.length();
    JSONPath& path = paths[j];
    if (!path.parsed)
    {
      mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &path.p.steps, sizeof(json_path_step_t), &p_steps,
                              JSON_DEPTH_LIMIT, 0, MYF(0));
      json_path_step_t *last_step= (json_path_step_t*)(mem_root_dynamic_array_get_val(&path.p.steps, path.p.last_step_idx)),
      *initial_step= (json_path_step_t*)path.p.steps.buffer;
      if (parseJSPath(path, row, fp[i]) || (last_step - 1) < initial_step ||
          last_step->type != JSON_PATH_ARRAY)
      {
        if (path.p.s.error == 0)
          path.p.s.error = SHOULD_END_WITH_ARRAY;
        goto error;
      }
      path.p.last_step_idx--;
    }

    initJSEngine(jsEg, cs, tmpJS);


    int jsErr = 0;
    if (locateJSPath(jsEg, path, &jsErr))
    {
      if (jsErr)
        goto error;

      // Can't find the array to insert.
      continue;
    }

    if (json_read_value(&jsEg))
      goto error;

    if (jsEg.value_type != JSON_VALUE_ARRAY)
    {
      /* Must be an array. */
      continue;
    }

    const char* itemPos = 0;
    IntType itemSize = 0;

    while (json_scan_next(&jsEg) == 0 && jsEg.state != JST_ARRAY_END)
    {
      DBUG_ASSERT(jsEg.state == JST_VALUE);
      if (itemSize == (((json_path_step_t*)
                        (mem_root_dynamic_array_get_val(&path.p.steps,
                                                        path.p.last_step_idx)))[1].n_item))
      {
        itemPos = (const char*)jsEg.s.c_str;
        break;
      }
      itemSize++;

      if (json_read_value(&jsEg) || (!json_value_scalar(&jsEg) && json_skip_level(&jsEg)))
        goto error;
    }

    if (unlikely(jsEg.s.error || *jsEg.killed_ptr))
      goto error;

    if (itemPos)
    {
      retJS.append(rawJS, itemPos - rawJS);
      if (itemSize > 0)
        retJS.append(" ");
      if (appendJSValue(retJS, cs, row, fp[i + 1]))
        goto error;
      retJS.append(",");
      if (itemSize == 0)
        retJS.append(" ");
      retJS.append(itemPos, rawJS + jsLen - itemPos);
    }
    else
    {
      /* Insert position wasn't found - append to the array. */
      DBUG_ASSERT(jsEg.state == JST_ARRAY_END);
      itemPos = (const char*)(jsEg.s.c_str - jsEg.sav_c_len);
      retJS.append(rawJS, itemPos - rawJS);
      if (itemSize > 0)
        retJS.append(", ");
      if (appendJSValue(retJS, cs, row, fp[i + 1]))
        goto error;
      retJS.append(itemPos, rawJS + jsLen - itemPos);
    }

    // tmpJS save the json string for next loop
    tmpJS.assign(retJS);
    retJS.clear();
  }

  initJSEngine(jsEg, cs, tmpJS);
  retJS.clear();
  if (doFormat(&jsEg, retJS, Func_json_format::LOOSE))
    goto error;

  isNull = false;
  return retJS;

error:
  isNull = true;
  return "";
}

}  // namespace funcexp
