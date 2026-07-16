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
                                                              CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_remove::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                        execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto& js = fp[0]->data()->getStrVal(row, isNull);

  if (isNull)
    return "";

  Func_json_multipath_state state(fp, 1, 1);

  int jsErr = 0;
  json_string_t keyName;
  const CHARSET_INFO* cs = getCharset(fp[0]);
  json_string_set_cs(&keyName, cs);

  std::string retJS;
  utils::NullString tmpJS(js);

  for (size_t i = 1, j = 0; i < fp.size(); i++, j++)
  {
    const char* rawJS = tmpJS.str();
    const size_t jsLen = tmpJS.length();

    JSONPath& path = state.paths[j];
    const json_path_step_t* lastStep;
    const char *remStart = nullptr, *remEnd = nullptr;
    IntType itemSize = 0;

#if MYSQL_VERSION_ID >= 120200
    json_path_step_t *curr_last_step= nullptr;
#endif

    if (!path.parsed)
    {
      if (parseJSPath(path, row, fp[i], false))
        goto error;

#if MYSQL_VERSION_ID >= 120200
      path.p.last_step_idx--;
      curr_last_step= reinterpret_cast<json_path_step_t*>
                                    (mem_root_dynamic_array_get_val(&path.p.steps,
                                                                    path.p.last_step_idx));
      if (curr_last_step < reinterpret_cast<json_path_step_t*>(path.p.steps.buffer))
#else
       path.p.last_step--;
      if (path.p.last_step < path.p.steps)
#endif
      {
        path.p.s.error = TRIVIAL_PATH_NOT_ALLOWED;
        goto error;
      }
    }

    initJSEngine(state.jsEg, cs, tmpJS);

#if MYSQL_VERSION_ID >= 120200
    if (curr_last_step < reinterpret_cast<json_path_step_t*>(path.p.steps.buffer))
#else
    if (path.p.last_step < path.p.steps)
#endif
      goto v_found;

    if (locateJSPath(state.jsEg, path, &jsErr) && jsErr)
      goto error;

    if (json_read_value(&state.jsEg))
      goto error;

#if MYSQL_VERSION_ID >= 120200
    lastStep = curr_last_step + 1;
#else
    lastStep = path.p.last_step + 1;
#endif

    if (lastStep->type & JSON_PATH_ARRAY)
    {
      if (state.jsEg.value_type != JSON_VALUE_ARRAY)
        continue;

      while (json_scan_next(&state.jsEg) == 0 && state.jsEg.state != JST_ARRAY_END)
      {
        switch (state.jsEg.state)
        {
          case JST_VALUE:
            if (itemSize == lastStep->n_item)
            {
              remStart = (const char*)(state.jsEg.s.c_str - (itemSize ? state.jsEg.sav_c_len : 0));
              goto v_found;
            }
            itemSize++;
            if (json_skip_array_item(&state.jsEg))
              goto error;
            break;
          default: break;
        }
      }

      if (unlikely(state.jsEg.s.error))
        goto error;

      continue;
    }
    else /*JSON_PATH_KEY*/
    {
      if (state.jsEg.value_type != JSON_VALUE_OBJECT)
        continue;

      while (json_scan_next(&state.jsEg) == 0 && state.jsEg.state != JST_OBJ_END)
      {
        switch (state.jsEg.state)
        {
          case JST_KEY:
            if (itemSize == 0)
              remStart = (const char*)(state.jsEg.s.c_str - state.jsEg.sav_c_len);
            json_string_set_str(&keyName, lastStep->key, lastStep->key_end);
            if (json_key_matches(&state.jsEg, &keyName))
              goto v_found;

            if (json_skip_key(&state.jsEg))
              goto error;

            remStart = (const char*)state.jsEg.s.c_str;
            itemSize++;
            break;
          default: break;
        }
      }

      if (unlikely(state.jsEg.s.error))
        goto error;

      continue;
    }

  v_found:

    if (json_skip_key(&state.jsEg) || json_scan_next(&state.jsEg))
      goto error;
    remEnd = (state.jsEg.state == JST_VALUE && itemSize == 0) ? (const char*)state.jsEg.s.c_str
                                                        : (const char*)(state.jsEg.s.c_str - state.jsEg.sav_c_len);
    retJS.clear();
    retJS.append(rawJS, remStart - rawJS);
    if (state.jsEg.state == JST_KEY && itemSize > 0)
      retJS.append(",");
    retJS.append(remEnd, rawJS + jsLen - remEnd);

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
