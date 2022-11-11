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
static bool appendJSPath(string& ret, const json_path_t* p)
{
  const json_path_step_t* c;

  try
  {
    ret.append("\"$");

    for (c = p->steps + 1; c <= p->last_step; c++)
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
int Func_json_search::cmpJSValWild(json_engine_t* jsEg, const string_view& cmpStr, const CHARSET_INFO* cs)
{
  if (jsEg->value_type != JSON_VALUE_STRING || !jsEg->value_escaped)
    return cs->wildcmp((const char*)jsEg->value, (const char*)(jsEg->value + jsEg->value_len),
                       (const char*)cmpStr.data(), (const char*)cmpStr.data() + cmpStr.size(), escape,
                       wildOne, wildMany)
               ? 0
               : 1;

  {
    int strLen = (jsEg->value_len / 1024 + 1) * 1024;
    char* buf = (char*)alloca(strLen);

    if ((strLen = json_unescape(jsEg->s.cs, jsEg->value, jsEg->value + jsEg->value_len, jsEg->s.cs,
                                (uchar*)buf, (uchar*)(buf + strLen))) <= 0)
      return 0;

    return cs->wildcmp(buf, buf + strLen, cmpStr.data(), cmpStr.data() + cmpStr.size(), escape, wildOne,
                       wildMany)
               ? 0
               : 1;
  }
}

CalpontSystemCatalog::ColType Func_json_search::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_search::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& type)
{
  string ret;
  bool isNullJS = false, isNullVal = false;
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  const string_view cmpStr = fp[2]->data()->getStrVal(row, isNull);
  if (isNullJS || isNullVal)
  {
    isNull = true;
    return "";
  }

  if (!isModeParsed)
  {
    if (!isModeConst)
      isModeConst = (dynamic_cast<ConstantColumn*>(fp[1]->data()) != nullptr);

    string mode = fp[1]->data()->getStrVal(row, isNull);
    if (isNull)
      return "";

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
    const string_view escapeStr = fp[3]->data()->getStrVal(row, isNullEscape);
    if (escapeStr.size() > 1)
    {
      isNull = true;
      return "";
    }
    escape = isNullEscape ? '\\' : escapeStr[0];
  }

  json_engine_t jsEg;
  json_path_t p, savPath;
  const CHARSET_INFO* cs = getCharset(fp[0]);

#ifdef MYSQL_GE_1009
  int arrayCounter[JSON_DEPTH_LIMIT];
  bool hasNegPath = 0;
#endif
  int pathFound = 0;

  initJSPaths(paths, fp, 4, 1);

  for (size_t i = 4; i < fp.size(); i++)
  {
    JSONPath& path = paths[i - 4];
    if (!path.parsed)
    {
      if (parseJSPath(path, row, fp[i]))
        goto error;
#ifdef MYSQL_GE_1009
      hasNegPath |= path.p.types_used & JSON_PATH_NEGATIVE_INDEX;
#endif
    }
  }

  json_get_path_start(&jsEg, cs, (const uchar*)js.data(), (const uchar*)js.data() + js.size(), &p);

  while (json_get_path_next(&jsEg, &p) == 0)
  {
#ifdef MYSQL_GE_1009
    if (hasNegPath && jsEg.value_type == JSON_VALUE_ARRAY &&
        json_skip_array_and_count(&jsEg, arrayCounter + (p.last_step - p.steps)))
      goto error;
#endif

    if (json_value_scalar(&jsEg))
    {
#ifdef MYSQL_GE_1009
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
          savPath.last_step = savPath.steps + (p.last_step - p.steps);
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
