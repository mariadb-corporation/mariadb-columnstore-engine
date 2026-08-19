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

namespace
{
static bool appendJSPath(std::string& ret, const json_path_t* p)
{
  const json_path_step_t* c;
#if MYSQL_VERSION_ID >= 120200
  const json_path_step_t* last_step= (const json_path_step_t*)
                                           (mem_root_dynamic_array_get_val((MEM_ROOT_DYNAMIC_ARRAY*)&p->steps,
                                                                            (size_t)p->last_step_idx));
#endif
  try
  {
    ret.append("\"$");

#if MYSQL_VERSION_ID >= 120200
    for (c = reinterpret_cast<json_path_step_t*>(p->steps.buffer)+1; c <= last_step; c++)
#else
    for (c = p->steps + 1; c <= p->last_step; c++)
#endif
    {
      if (c->type & JSON_PATH_KEY)
      {
        ret.append(".", 1);
        ret.append((const char*)c->key, c->key_end - c->key);
      }
      else /*JSON_PATH_ARRAY*/
      {
        ret.append("[");
        ret.append(std::to_string(c->n_item));
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
int Func_json_search::cmpJSValWild(Func_json_search_state& state, json_engine_t* jsEg, const utils::NullString& cmpStr,
                                   const CHARSET_INFO* cs)
{
  if (jsEg->value_type != JSON_VALUE_STRING || !jsEg->value_escaped)
    return cs->wildcmp((const char*)jsEg->value, (const char*)(jsEg->value + jsEg->value_len),
                       (const char*)cmpStr.str(), (const char*)cmpStr.end(), state.escape, wildOne, wildMany)
               ? 0
               : 1;

  {
    int strLen = (jsEg->value_len / 1024 + 1) * 1024;
    char* buf = (char*)alloca(strLen);

    if ((strLen = json_unescape(jsEg->s.cs, jsEg->value, jsEg->value + jsEg->value_len, jsEg->s.cs,
                                (uchar*)buf, (uchar*)(buf + strLen))) <= 0)
      return 0;

    return cs->wildcmp(buf, buf + strLen, cmpStr.str(), cmpStr.end(), state.escape, wildOne, wildMany) ? 0 : 1;
  }
}

CalpontSystemCatalog::ColType Func_json_search::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_search::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                        execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  std::string ret;
  bool isNullJS = false, isNullVal = false;
  const auto& js = fp[0]->data()->getStrVal(row, isNull);
  const auto& cmpStr = fp[2]->data()->getStrVal(row, isNull);
  if (isNullJS || isNullVal)
  {
    isNull = true;
    return "";
  }

  Func_json_search_state state(fp);

  if (!state.isModeParsed)
  {
    if (!state.isModeConst)
      state.isModeConst = (dynamic_cast<ConstantColumn*>(fp[1]->data()) != nullptr);

    const auto& mode_ns = fp[1]->data()->getStrVal(row, isNull);
    if (isNull)
      return "";
    std::string mode = mode_ns.safeString("");

    transform(mode.begin(), mode.end(), mode.begin(), ::tolower);
    if (mode != "one" && mode != "all")
    {
      isNull = true;
      return "";
    }

    state.isModeOne = (mode == "one");
    state.isModeParsed = state.isModeConst;
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
    state.escape = isNullEscape ? '\\' : escapeStr.safeString("")[0];
  }

  const CHARSET_INFO* cs = getCharset(fp[0]);

#if MYSQL_VERSION_ID >= 100900
  int arrayCounter[JSON_DEPTH_LIMIT];
  bool hasNegPath = 0;
#endif
  int pathFound = 0;

  std::string firstValue;

  for (size_t i = 4; i < fp.size(); i++)
  {
    JSONPath& path = state.paths[i - 4];

    if (!path.parsed)
    {
      if (parseJSPath(path, row, fp[i]))
        goto error;
#if MYSQL_VERSION_ID >= 100900
      hasNegPath |= path.p.types_used & JSON_PATH_NEGATIVE_INDEX;
#endif
    }
  }

  json_get_path_start(&state.jsEg, cs, (const uchar*)js.str(), (const uchar*)js.end(), &state.p);

  while (json_get_path_next(&state.jsEg, &state.p) == 0)
  {
#if MYSQL_VERSION_ID >= 100900
#if MYSQL_VERSION_ID >= 120200
    if (hasNegPath && state.jsEg.value_type == JSON_VALUE_ARRAY &&
        json_skip_array_and_count(&state.jsEg, arrayCounter + state.p.last_step_idx))
#else
        if (hasNegPath && state.jsEg.value_type == JSON_VALUE_ARRAY &&
        json_skip_array_and_count(&state.jsEg, arrayCounter + (state.p.last_step - state.p.steps)))
#endif
      goto error;
#endif

    if (json_value_scalar(&state.jsEg))
    {
#if MYSQL_VERSION_ID >= 100900
      bool isMatch = matchJSPath(state.paths, &state.p, state.jsEg.value_type, arrayCounter);
#else
      bool isMatch = matchJSPath(state.paths, &state.p, state.jsEg.value_type);
#endif
      if ((fp.size() < 5 || isMatch) && cmpJSValWild(state, &state.jsEg, cmpStr, cs) != 0)
      {
        ++pathFound;
        if (pathFound == 1)
        {
          if (appendJSPath(firstValue, &state.p))
          {
            goto error;
          }
        }
        else
        {
          if (pathFound == 2)
          {
            ret.append("[");
            ret.append(firstValue);
          }
          ret.append(", ");
          if (appendJSPath(ret, &state.p))
            goto error;
        }
        if (state.isModeOne)
          goto end;
      }
    }
  }

end:
  if (pathFound == 0)
    goto error;
  if (pathFound == 1)
  {
    ret = firstValue;
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
