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
CalpontSystemCatalog::ColType Func_json_insert::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_insert::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  const bool isInsertMode = mode == INSERT || mode == SET;
  const bool isReplaceMode = mode == REPLACE || mode == SET;

  json_engine_t jsEg;

  int jsErr = 0;
  json_string_t keyName;
  const CHARSET_INFO* cs = getCharset(fp[0]);
  json_string_set_cs(&keyName, cs);

  initJSPaths(paths, fp, 1, 2);

  // Save the result of each merge and the result of the final merge separately
  string retJS;
  string tmpJS{js};
  for (size_t i = 1, j = 0; i < fp.size(); i += 2, j++)
  {
    const char* rawJS = tmpJS.data();
    const size_t jsLen = tmpJS.size();

    JSONPath& path = paths[j];
    const json_path_step_t* lastStep;
    const char* valEnd;

    if (!path.parsed)
    {
      if (parseJSPath(path, row, fp[i], false))
        goto error;

      path.p.last_step--;
    }

    initJSEngine(jsEg, cs, tmpJS);
    if (path.p.last_step < path.p.steps)
      goto v_found;

    if (path.p.last_step >= path.p.steps && locateJSPath(jsEg, path, &jsErr))
    {
      if (jsErr)
        goto error;
      continue;
    }

    if (json_read_value(&jsEg))
      goto error;

    lastStep = path.p.last_step + 1;
    if (lastStep->type & JSON_PATH_ARRAY)
    {
      IntType itemSize = 0;

      if (jsEg.value_type != JSON_VALUE_ARRAY)
      {
        const uchar* valStart = jsEg.value_begin;
        bool isArrAutoWrap;

        if (isInsertMode)
        {
          if (isReplaceMode)
            isArrAutoWrap = lastStep->n_item > 0;
          else
          {
            if (lastStep->n_item == 0)
              continue;
            isArrAutoWrap = true;
          }
        }
        else
        {
          if (lastStep->n_item)
            continue;
          isArrAutoWrap = false;
        }

        retJS.clear();
        /* Wrap the value as an array. */
        retJS.append(rawJS, (const char*)valStart - rawJS);
        if (isArrAutoWrap)
          retJS.append("[");

        if (jsEg.value_type == JSON_VALUE_OBJECT)
        {
          if (json_skip_level(&jsEg))
            goto error;
        }

        if (isArrAutoWrap)
          retJS.append((const char*)valStart, jsEg.s.c_str - valStart);
        retJS.append(", ");
        if (appendJSValue(retJS, cs, row, fp[i + 1]))
          goto error;
        if (isArrAutoWrap)
          retJS.append("]");
        retJS.append((const char*)jsEg.s.c_str, rawJS + jsLen - (const char*)jsEg.s.c_str);

        goto continue_point;
      }

      while (json_scan_next(&jsEg) == 0 && jsEg.state != JST_ARRAY_END)
      {
        switch (jsEg.state)
        {
          case JST_VALUE:
            if (itemSize == lastStep->n_item)
              goto v_found;
            itemSize++;
            if (json_skip_array_item(&jsEg))
              goto error;
            break;
          default: break;
        }
      }

      if (unlikely(jsEg.s.error))
        goto error;

      if (!isInsertMode)
        continue;

      valEnd = (const char*)(jsEg.s.c_str - jsEg.sav_c_len);
      retJS.clear();
      retJS.append(rawJS, valEnd - rawJS);
      if (itemSize > 0)
        retJS.append(", ");
      if (appendJSValue(retJS, cs, row, fp[i + 1]))
        goto error;
      retJS.append(valEnd, rawJS + jsLen - valEnd);
    }
    else /*JSON_PATH_KEY*/
    {
      IntType keySize = 0;

      if (jsEg.value_type != JSON_VALUE_OBJECT)
        continue;

      while (json_scan_next(&jsEg) == 0 && jsEg.state != JST_OBJ_END)
      {
        switch (jsEg.state)
        {
          case JST_KEY:
            json_string_set_str(&keyName, lastStep->key, lastStep->key_end);
            if (json_key_matches(&jsEg, &keyName))
              goto v_found;
            keySize++;
            if (json_skip_key(&jsEg))
              goto error;
            break;
          default: break;
        }
      }

      if (unlikely(jsEg.s.error))
        goto error;

      if (!isInsertMode)
        continue;

      valEnd = (const char*)(jsEg.s.c_str - jsEg.sav_c_len);

      retJS.clear();
      retJS.append(rawJS, valEnd - rawJS);

      if (keySize > 0)
        retJS.append(", ");

      retJS.append("\"");
      retJS.append((const char*)lastStep->key, lastStep->key_end - lastStep->key);
      retJS.append("\":");

      if (appendJSValue(retJS, cs, row, fp[i + 1]))
        goto error;
      retJS.append(valEnd, rawJS + jsLen - valEnd);
    }

    goto continue_point;

  v_found:

    if (!isReplaceMode)
      continue;

    if (json_read_value(&jsEg))
      goto error;

    valEnd = (const char*)jsEg.value_begin;
    retJS.clear();
    if (!json_value_scalar(&jsEg))
    {
      if (json_skip_level(&jsEg))
        goto error;
    }

    retJS.append(rawJS, valEnd - rawJS);
    if (appendJSValue(retJS, cs, row, fp[i + 1]))
      goto error;
    retJS.append((const char*)jsEg.s.c_str, rawJS + jsLen - (const char*)jsEg.s.c_str);

  continue_point:
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
