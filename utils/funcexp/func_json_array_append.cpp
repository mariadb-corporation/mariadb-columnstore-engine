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
CalpontSystemCatalog::ColType Func_json_array_append::operationType(FunctionParm& fp,
                                                                    CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_array_append::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                         execplan::CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  const CHARSET_INFO* cs = getCharset(fp[0]);

  json_engine_t jsEg;
  const uchar* arrEnd;
  size_t strRestLen;
  string retJS;
  retJS.reserve(js.size() + padding);

  initJSPaths(paths, fp, 1, 2);

  string tmpJS{js};
  for (size_t i = 1, j = 0; i < fp.size(); i += 2, j++)
  {
    const char* rawJS = tmpJS.data();
    const size_t jsLen = tmpJS.size();
    JSONPath& path = paths[j];
    if (!path.parsed && parseJSPath(path, row, fp[i], false))
      goto error;

    initJSEngine(jsEg, cs, tmpJS);

    if (locateJSPath(jsEg, path))
      goto error;

    if (json_read_value(&jsEg))
      goto error;

    if (jsEg.value_type == JSON_VALUE_ARRAY)
    {
      int itemSize;
      if (json_skip_level_and_count(&jsEg, &itemSize))
        goto error;

      arrEnd = jsEg.s.c_str - jsEg.sav_c_len;
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
      retJS.append(rawJS, (const char*)jsEg.value_begin - rawJS);
      start = jsEg.value_begin;

      if (jsEg.value_type == JSON_VALUE_OBJECT)
      {
        if (json_skip_level(&jsEg))
          goto error;
        end = jsEg.s.c_str;
      }
      else
        end = jsEg.value_end;

      retJS.append("[");
      retJS.append((const char*)start, end - start);
      retJS.append(", ");
      if (appendJSValue(retJS, cs, row, fp[i + 1]))
        goto error;
      retJS.append("]");
      retJS.append((const char*)jsEg.s.c_str, rawJS + jsLen - (const char*)jsEg.s.c_str);
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
