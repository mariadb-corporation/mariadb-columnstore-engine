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
                                                              CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_insert::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                        execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto& js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  Func_json_multipath_state state(fp, 1, 2);

  const bool isInsertMode = mode == INSERT || mode == SET;
  const bool isReplaceMode = mode == REPLACE || mode == SET;

  int jsErr = 0;
  json_string_t keyName;
  const CHARSET_INFO* cs = getCharset(fp[0]);
  json_string_set_cs(&keyName, cs);

  // Save the result of each merge and the result of the final merge separately
  std::string retJS;
  utils::NullString tmpJS(js);
  for (size_t i = 1, j = 0; i < fp.size(); i += 2, j++)
  {
    const char* rawJS = tmpJS.str();
    const size_t jsLen = tmpJS.length();

    JSONPath& path = state.paths[j];
    const json_path_step_t* lastStep;
    const char* valEnd;

    if (!path.parsed)
    {
      if (parseJSPath(path, row, fp[i], false))
        goto error;

#if MYSQL_VERSION_ID >= 120200
      path.p.last_step_idx--;
#else
      path.p.last_step--;
#endif
    }

    initJSEngine(state.jsEg, cs, tmpJS);

#if MYSQL_VERSION_ID >= 120200
    if (path.p.last_step_idx < 0)
#else
    if (path.p.last_step < path.p.steps)
#endif
      goto v_found;

#if MYSQL_VERSION_ID >= 120200
    if (path.p.last_step_idx >= 0 && locateJSPath(state.jsEg, path, &jsErr))
#else
    if (path.p.last_step >= path.p.steps && locateJSPath(state.jsEg, path, &jsErr))
#endif
    {
      if (jsErr)
        goto error;
      continue;
    }

    if (json_read_value(&state.jsEg))
      goto error;

#if MYSQL_VERSION_ID >= 120200
    if (path.p.last_step_idx < 0)
    {
      lastStep= (reinterpret_cast<json_path_step_t*>
                        (mem_root_dynamic_array_get_val(&path.p.steps,
                             0))) + path.p.last_step_idx + 1;
    }
    else
    {
      lastStep= (reinterpret_cast<json_path_step_t*>
                        (mem_root_dynamic_array_get_val(&path.p.steps,
                             path.p.last_step_idx))) + 1;
    }
#else
    lastStep = path.p.last_step + 1;
#endif

    if (lastStep->type & JSON_PATH_ARRAY)
    {
      IntType itemSize = 0;

      if (state.jsEg.value_type != JSON_VALUE_ARRAY)
      {
        const uchar* valStart = state.jsEg.value_begin;
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

        if (state.jsEg.value_type == JSON_VALUE_OBJECT)
        {
          if (json_skip_level(&state.jsEg))
            goto error;
        }

        if (isArrAutoWrap)
          retJS.append((const char*)valStart, state.jsEg.s.c_str - valStart);
        if (retJS.length() > 0)
          retJS.append(", ");
        if (appendJSValue(retJS, cs, row, fp[i + 1]))
          goto error;
        if (isArrAutoWrap)
          retJS.append("]");
        retJS.append((const char*)state.jsEg.s.c_str, rawJS + jsLen - (const char*)state.jsEg.s.c_str);

        goto continue_point;
      }

      while (json_scan_next(&state.jsEg) == 0 && state.jsEg.state != JST_ARRAY_END)
      {
        switch (state.jsEg.state)
        {
          case JST_VALUE:
            if (itemSize == lastStep->n_item)
              goto v_found;
            itemSize++;
            if (json_skip_array_item(&state.jsEg))
              goto error;
            break;
          default: break;
        }
      }

      if (unlikely(state.jsEg.s.error))
        goto error;

      if (!isInsertMode)
        continue;

      valEnd = (const char*)(state.jsEg.s.c_str - state.jsEg.sav_c_len);
      retJS.clear();
      retJS.append(rawJS, valEnd - rawJS);
      if (itemSize > 0 && retJS.length() > 0)
        retJS.append(", ");
      if (appendJSValue(retJS, cs, row, fp[i + 1]))
        goto error;
      retJS.append(valEnd, rawJS + jsLen - valEnd);
    }
    else /*JSON_PATH_KEY*/
    {
      IntType keySize = 0;

      if (state.jsEg.value_type != JSON_VALUE_OBJECT)
        continue;

      while (json_scan_next(&state.jsEg) == 0 && state.jsEg.state != JST_OBJ_END)
      {
        switch (state.jsEg.state)
        {
          case JST_KEY:
            json_string_set_str(&keyName, lastStep->key, lastStep->key_end);
            if (json_key_matches(&state.jsEg, &keyName))
              goto v_found;
            keySize++;
            if (json_skip_key(&state.jsEg))
              goto error;
            break;
          default: break;
        }
      }

      if (unlikely(state.jsEg.s.error))
        goto error;

      if (!isInsertMode)
        continue;

      valEnd = (const char*)(state.jsEg.s.c_str - state.jsEg.sav_c_len);

      retJS.clear();
      retJS.append(rawJS, valEnd - rawJS);

      if (keySize > 0 && retJS.length() > 0)
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

    if (json_read_value(&state.jsEg))
      goto error;

    valEnd = (const char*)state.jsEg.value_begin;
    retJS.clear();
    if (!json_value_scalar(&state.jsEg))
    {
      if (json_skip_level(&state.jsEg))
        goto error;
    }

    retJS.append(rawJS, valEnd - rawJS);
    if (appendJSValue(retJS, cs, row, fp[i + 1]))
      goto error;
    retJS.append((const char*)state.jsEg.s.c_str, rawJS + jsLen - (const char*)state.jsEg.s.c_str);

  continue_point:
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
