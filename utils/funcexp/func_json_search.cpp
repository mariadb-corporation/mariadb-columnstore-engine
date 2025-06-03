#include <string_view>
using namespace std;

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

#include "my_sys.h"

namespace
{
static bool appendJSPath(string& ret, json_path_t* p)
{
  const json_path_step_t* c;
  json_path_step_t *last_step= (json_path_step_t*)(mem_root_dynamic_array_get_val(&p->steps, p->last_step_idx));

  try
  {
    ret.append("\"$");

    for (c = ((json_path_step_t*)(p->steps.buffer)) + 1; c <= last_step; c++)
    {
      if (c->type & JSON_PATH_KEY)
      {
        ret.append(".", 1);
        ret.append((const char*)c->key, c->key_end - c->key);
      }
      else /*JSON_PATH_ARRAY*/
      {
        ret.append("[");
        ret.append(to_string(c->n_item));
        ret.append("]");
      }
    }

    ret.append("\"");
  }
  catch (...)
  {
    return true;
  }

  return false;
}
}  // namespace
namespace funcexp
{
const static int wildOne = '_';
const static int wildMany = '%';
int Func_json_search::cmpJSValWild(json_engine_t* jsEg, const utils::NullString& cmpStr,
                                   const CHARSET_INFO* cs)
{
  if (jsEg->value_type != JSON_VALUE_STRING || !jsEg->value_escaped)
    return cs->wildcmp((const char*)jsEg->value, (const char*)(jsEg->value + jsEg->value_len),
                       (const char*)cmpStr.str(), (const char*)cmpStr.end(), escape, wildOne, wildMany)
               ? 0
               : 1;

  {
    int strLen = (jsEg->value_len / 1024 + 1) * 1024;
    char* buf = (char*)alloca(strLen);

    if ((strLen = json_unescape(jsEg->s.cs, jsEg->value, jsEg->value + jsEg->value_len, jsEg->s.cs,
                                (uchar*)buf, (uchar*)(buf + strLen))) <= 0)
      return 0;

    return cs->wildcmp(buf, buf + strLen, cmpStr.str(), cmpStr.end(), escape, wildOne, wildMany) ? 0 : 1;
  }
}

CalpontSystemCatalog::ColType Func_json_search::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

string Func_json_search::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  string ret;
  bool isNullJS = false, isNullVal = false;
  const auto& js = fp[0]->data()->getStrVal(row, isNull);
  const auto& cmpStr = fp[2]->data()->getStrVal(row, isNull);
  if (isNullJS || isNullVal)
  {
    isNull = true;
    return "";
  }

  if (!isModeParsed)
  {
    if (!isModeConst)
      isModeConst = (dynamic_cast<ConstantColumn*>(fp[1]->data()) != nullptr);

    const auto& mode_ns = fp[1]->data()->getStrVal(row, isNull);
    if (isNull)
      return "";
    string mode = mode_ns.safeString("");

    transform(mode.begin(), mode.end(), mode.begin(), ::tolower);
    if (mode != "one" && mode != "all")
    {
      isNull = true;
      return "";
    }

    isModeOne = (mode == "one");
    isModeParsed = isModeConst;
  }

  if (fp.size() >= 4)
  {
    if (dynamic_cast<ConstantColumn*>(fp[3]->data()) == nullptr)
    {
      isNull = true;
      return "";
    }
    bool isNullEscape = false;
    const auto& escapeStr = fp[3]->data()->getStrVal(row, isNullEscape);
    if (escapeStr.length() > 1)
    {
      isNull = true;
      return "";
    }
    escape = isNullEscape ? '\\' : escapeStr.safeString("")[0];
  }

  json_engine_t jsEg;
  int jsEg_stack[JSON_DEPTH_LIMIT];
  json_path_t p, savPath;
  json_path_step_t savPath_steps[JSON_DEPTH_LIMIT], p_steps[JSON_DEPTH_LIMIT];
  const CHARSET_INFO* cs = getCharset(fp[0]);

#if MYSQL_VERSION_ID >= 100900
  int arrayCounter[JSON_DEPTH_LIMIT];
  bool hasNegPath = 0;
#endif
  int pathFound = 0;

  initJSPaths(paths, fp, 4, 1);
  vector<vector<json_path_step_t>> p_steps_arr(paths.size(), vector<json_path_step_t>(32));

  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &savPath.steps, sizeof(json_path_step_t), &savPath_steps,
                              JSON_DEPTH_LIMIT, 0, MYF(0));
  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &p.steps, sizeof(json_path_step_t), &p_steps,
                              JSON_DEPTH_LIMIT, 0, MYF(0));

  for (size_t i = 4; i < fp.size(); i++)
  {
    JSONPath& path = paths[i - 4];
    if (!path.parsed)
    {
      mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &path.p.steps, sizeof(json_path_step_t), &p_steps_arr[i-4],
                              JSON_DEPTH_LIMIT, 0, MYF(0));
      if (parseJSPath(path, row, fp[i]))
        goto error;
#if MYSQL_VERSION_ID >= 100900
      hasNegPath |= path.p.types_used & JSON_PATH_NEGATIVE_INDEX;
#endif
    }
  }

  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &jsEg.stack, sizeof(int), &jsEg_stack,
                              JSON_DEPTH_LIMIT, 0, MYF(0));

  json_get_path_start(&jsEg, cs, (const uchar*)js.str(), (const uchar*)js.end(), &p);

  while (json_get_path_next(&jsEg, &p) == 0)
  {
#if MYSQL_VERSION_ID >= 100900
    if (hasNegPath && jsEg.value_type == JSON_VALUE_ARRAY &&
        json_skip_array_and_count(&jsEg, arrayCounter + (((json_path_step_t*)(mem_root_dynamic_array_get_val(&p.steps, p.last_step_idx))) - (json_path_step_t*)p.steps.buffer)))
      goto error;
#endif

    if (json_value_scalar(&jsEg))
    {
#if MYSQL_VERSION_ID >= 100900
      bool isMatch = matchJSPath(paths, &p, jsEg.value_type, arrayCounter);
#else
      bool isMatch = matchJSPath(paths, &p, jsEg.value_type);
#endif
      if ((fp.size() < 5 || isMatch) && cmpJSValWild(&jsEg, cmpStr, cs) != 0)
      {
        ++pathFound;
        if (pathFound == 1)
        {
          savPath = p;
        }
        else
        {
          if (pathFound == 2)
          {
            ret.append("[");
            if (appendJSPath(ret, &savPath))
              goto error;
          }
          ret.append(", ");
          if (appendJSPath(ret, &p))
            goto error;
        }
        if (isModeOne)
          goto end;
      }
    }
  }

end:
  if (pathFound == 0)
    goto error;
  if (pathFound == 1)
  {
    if (appendJSPath(ret, &savPath))
      goto error;
  }
  else
    ret.append("]");

  isNull = false;
  return ret;

error:
  isNull = true;
  return "";
}
}  // namespace funcexp
