#include "functor_json.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_array_insert::operationType(FunctionParm& fp,
                                                                    CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_array_insert::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                         execplan::CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  const CHARSET_INFO* cs = getCharset(fp[0]);

  json_engine_t jsEg;
  string retJS;
  retJS.reserve(js.size() + 8);

  initJSPaths(paths, fp, 1, 2);

  string tmpJS{js};
  for (size_t i = 1, j = 0; i < fp.size(); i += 2, j++)
  {
    const char* rawJS = tmpJS.data();
    const size_t jsLen = tmpJS.size();
    JSONPath& path = paths[j];
    if (!path.parsed)
    {
      if (parseJSPath(path, row, fp[i]) || path.p.last_step - 1 < path.p.steps ||
          path.p.last_step->type != JSON_PATH_ARRAY)
      {
        if (path.p.s.error == 0)
          path.p.s.error = SHOULD_END_WITH_ARRAY;
        goto error;
      }
      path.p.last_step--;
    }

    initJSEngine(jsEg, cs, tmpJS);

    path.currStep = path.p.steps;

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
      if (itemSize == path.p.last_step[1].n_item)
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
    tmpJS.swap(retJS);
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
