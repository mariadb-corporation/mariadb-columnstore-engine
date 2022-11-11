#include "functor_json.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "dataconvert.h"
using namespace dataconvert;

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_remove::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_remove::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  json_engine_t jsEg;

  int jsErr = 0;
  json_string_t keyName;
  const CHARSET_INFO* cs = getCharset(fp[0]);
  json_string_set_cs(&keyName, cs);

  initJSPaths(paths, fp, 1, 1);

  string retJS;
  string tmpJS{js};
  for (size_t i = 1, j = 0; i < fp.size(); i++, j++)
  {
    const char* rawJS = tmpJS.data();
    const size_t jsLen = tmpJS.size();

    JSONPath& path = paths[j];
    const json_path_step_t* lastStep;
    const char *remStart = nullptr, *remEnd = nullptr;
    IntType itemSize = 0;

    if (!path.parsed)
    {
      if (parseJSPath(path, row, fp[i], false))
        goto error;

      path.p.last_step--;
      if (path.p.last_step < path.p.steps)
      {
        path.p.s.error = TRIVIAL_PATH_NOT_ALLOWED;
        goto error;
      }
    }

    initJSEngine(jsEg, cs, rawJS);

    if (path.p.last_step < path.p.steps)
      goto v_found;

    if (locateJSPath(jsEg, path, &jsErr) && jsErr)
      goto error;

    if (json_read_value(&jsEg))
      goto error;

    lastStep = path.p.last_step + 1;
    if (lastStep->type & JSON_PATH_ARRAY)
    {
      if (jsEg.value_type != JSON_VALUE_ARRAY)
        continue;

      while (json_scan_next(&jsEg) == 0 && jsEg.state != JST_ARRAY_END)
      {
        switch (jsEg.state)
        {
          case JST_VALUE:
            if (itemSize == lastStep->n_item)
            {
              remStart = (const char*)(jsEg.s.c_str - (itemSize ? jsEg.sav_c_len : 0));
              goto v_found;
            }
            itemSize++;
            if (json_skip_array_item(&jsEg))
              goto error;
            break;
          default: break;
        }
      }

      if (unlikely(jsEg.s.error))
        goto error;

      continue;
    }
    else /*JSON_PATH_KEY*/
    {
      if (jsEg.value_type != JSON_VALUE_OBJECT)
        continue;

      while (json_scan_next(&jsEg) == 0 && jsEg.state != JST_OBJ_END)
      {
        switch (jsEg.state)
        {
          case JST_KEY:
            if (itemSize == 0)
              remStart = (const char*)(jsEg.s.c_str - jsEg.sav_c_len);
            json_string_set_str(&keyName, lastStep->key, lastStep->key_end);
            if (json_key_matches(&jsEg, &keyName))
              goto v_found;

            if (json_skip_key(&jsEg))
              goto error;

            remStart = (const char*)jsEg.s.c_str;
            itemSize++;
            break;
          default: break;
        }
      }

      if (unlikely(jsEg.s.error))
        goto error;

      continue;
    }

  v_found:

    if (json_skip_key(&jsEg) || json_scan_next(&jsEg))
      goto error;
    remEnd = (jsEg.state == JST_VALUE && itemSize == 0) ? (const char*)jsEg.s.c_str
                                                        : (const char*)(jsEg.s.c_str - jsEg.sav_c_len);
    retJS.clear();
    retJS.append(rawJS, remStart - rawJS);
    if (jsEg.state == JST_KEY && itemSize > 0)
      retJS.append(",");
    retJS.append(remEnd, rawJS + jsLen - remEnd);

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
