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
CalpontSystemCatalog::ColType Func_json_array_append::operationType(
    FunctionParm& fp, CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_array_append::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto& js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  const CHARSET_INFO* cs = getCharset(fp[0]);
  const uchar* arrEnd;
  size_t strRestLen;
  std::string retJS;
  retJS.reserve(js.length() + padding);

  utils::NullString tmpJS(js);
  Func_json_multipath_state state(fp, 1, 2);;
  for (size_t i = 1, j = 0; i < fp.size(); i += 2, j++)
  {
    const char* rawJS = tmpJS.str();
    const size_t jsLen = tmpJS.length();
    JSONPath& path = state.paths[j];

    if (!path.parsed && parseJSPath(path, row, fp[i], false))
      goto error;

    initJSEngine(state.jsEg, cs, tmpJS);

    if (locateJSPath(state.jsEg, path))
      goto error;

    if (json_read_value(&state.jsEg))
      goto error;

    if (state.jsEg.value_type == JSON_VALUE_ARRAY)
    {
      int itemSize;
      if (json_skip_level_and_count(&state.jsEg, &itemSize))
        goto error;

      arrEnd = state.jsEg.s.c_str - state.jsEg.sav_c_len;
      strRestLen = jsLen - (arrEnd - (const uchar*)rawJS);
      retJS.append(rawJS, arrEnd - (const uchar*)rawJS);
      if (itemSize)
        retJS.append(", ");
      if (appendJSValue(retJS, cs, row, fp[i + 1]))
        goto error;

      retJS.append((const char*)arrEnd, strRestLen);
    }
    else
    {
      const uchar *start, *end;

      /* Wrap as an array. */
      retJS.append(rawJS, (const char*)state.jsEg.value_begin - rawJS);
      start = state.jsEg.value_begin;
      if (state.jsEg.value_type == JSON_VALUE_OBJECT)
      {
        if (json_skip_level(&state.jsEg))
          goto error;
        end = state.jsEg.s.c_str;
      }
      else
        end = state.jsEg.value_end;

      retJS.append("[");
      retJS.append((const char*)start, end - start);
      retJS.append(", ");
      if (appendJSValue(retJS, cs, row, fp[i + 1]))
        goto error;
      retJS.append("]");
      retJS.append((const char*)state.jsEg.s.c_str, rawJS + jsLen - (const char*)state.jsEg.s.c_str);
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
