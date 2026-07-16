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

std::string Func_json_array_insert::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto& js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  const CHARSET_INFO* cs = getCharset(fp[0]);

  std::string retJS;

  retJS.reserve(js.length() + 8);

  utils::NullString tmpJS(js);
  Func_json_multipath_state state(fp, 1, 2);
  for (size_t i = 1, j = 0; i < fp.size(); i += 2, j++)
  {
    const char* rawJS = tmpJS.str();
    const size_t jsLen = tmpJS.length();
    JSONPath& path = state.paths[j];
    if (!path.parsed)
    {
      uint32_t last_type = 0;
      if (parseNonEmptyJSPath(path, row, fp[i], &last_type) || last_type != JSON_PATH_ARRAY)
      {
        if (path.p.s.error == 0)
          path.p.s.error = SHOULD_END_WITH_ARRAY;
        goto error;
      }
#if MYSQL_VERSION_ID >= 120200
      path.p.last_step_idx--;
#else
      path.p.last_step--;
#endif
    }

    initJSEngine(state.jsEg, cs, tmpJS);

#if MYSQL_VERSION_ID < 120100
    path.currStep = path.p.steps;
#endif

    int jsErr = 0;
    if (locateJSPath(state.jsEg, path, &jsErr))
    {
      if (jsErr)
        goto error;

      // Can't find the array to insert.
      continue;
    }

    if (json_read_value(&state.jsEg))
      goto error;

    if (state.jsEg.value_type != JSON_VALUE_ARRAY)
    {
      /* Must be an array. */
      continue;
    }

    const char* itemPos = 0;
    IntType itemSize = 0;

    while (json_scan_next(&state.jsEg) == 0 && state.jsEg.state != JST_ARRAY_END)
    {
      DBUG_ASSERT(state.jsEg.state == JST_VALUE);
#if MYSQL_VERSION_ID >= 120200
      if (itemSize == ((reinterpret_cast<json_path_step_t*>
                        (mem_root_dynamic_array_get_val(&path.p.steps,
                                                        path.p.last_step_idx)))[1].n_item))
#else
      if (itemSize == path.p.last_step[1].n_item)
#endif
      {
        itemPos = (const char*)state.jsEg.s.c_str;
        break;
      }
      itemSize++;

      if (json_read_value(&state.jsEg) || (!json_value_scalar(&state.jsEg) && json_skip_level(&state.jsEg)))
        goto error;
    }

    if (unlikely(state.jsEg.s.error || *state.jsEg.killed_ptr))
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
      DBUG_ASSERT(state.jsEg.state == JST_ARRAY_END);
      itemPos = (const char*)(state.jsEg.s.c_str - state.jsEg.sav_c_len);
      retJS.append(rawJS, itemPos - rawJS);
      if (itemSize > 0 && itemPos > rawJS)
        retJS.append(", ");
      if (appendJSValue(retJS, cs, row, fp[i + 1]))
        goto error;
      retJS.append(itemPos, rawJS + jsLen - itemPos);
    }

    // tmpJS save the json string for next loop
    tmpJS.assign(retJS);
    retJS.clear();
  }

  initJSEngine(state.jsEg, cs, tmpJS);
  retJS.clear();
  if (doFormat(&state.jsEg, retJS, Func_json_format::LOOSE))
    goto error;

  isNull = false;
  return retJS;

error:
  isNull = true;
  return "";
}

}  // namespace funcexp
