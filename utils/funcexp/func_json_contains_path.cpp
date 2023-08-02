#include <string_view>
#include <algorithm>
using namespace std;

#include "functor_json.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
#include "rowgroup.h"
using namespace execplan;
using namespace rowgroup;

#include "dataconvert.h"

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_contains_path::operationType(
    FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_contains_path::getBoolVal(Row& row, FunctionParm& fp, bool& isNull,
                                         CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

#ifdef MYSQL_GE_1009
  int arrayCounters[JSON_DEPTH_LIMIT];
  bool hasNegPath = false;
#endif
  const int argSize = fp.size() - 2;

  if (!isModeParsed)
  {
    if (!isModeConst)
      isModeConst = (dynamic_cast<ConstantColumn*>(fp[1]->data()) != nullptr);

    string mode = fp[1]->data()->getStrVal(row, isNull);
    if (isNull)
      return false;

    transform(mode.begin(), mode.end(), mode.begin(), ::tolower);
    if (mode != "one" && mode != "all")
    {
      isNull = true;
      return false;
    }

    isModeOne = (mode == "one");
    isModeParsed = isModeConst;
  }

  initJSPaths(paths, fp, 2, 1);
  if (paths.size() == 0)
    hasFound.assign(argSize, false);

  for (size_t i = 2; i < fp.size(); i++)
  {
    JSONPath& path = paths[i - 2];

    if (!path.parsed)
    {
      if (parseJSPath(path, row, fp[i]))
      {
        isNull = true;
        return false;
      }
#ifdef MYSQL_GE_1009
      hasNegPath |= path.p.types_used & JSON_PATH_NEGATIVE_INDEX;
#endif
    }
  }

  json_engine_t jsEg;
  json_path_t p;
  json_get_path_start(&jsEg, getCharset(fp[0]), (const uchar*)js.data(), (const uchar*)js.data() + js.size(),
                      &p);

  bool result = false;
  int needFound = 0;

  if (!isModeOne)
  {
    hasFound.assign(argSize, false);
    needFound = argSize;
  }

  while (json_get_path_next(&jsEg, &p) == 0)
  {
#ifdef MYSQL_GE_1009
    if (hasNegPath && jsEg.value_type == JSON_VALUE_ARRAY &&
        json_skip_array_and_count(&jsEg, arrayCounters + (p.last_step - p.steps)))
    {
      result = true;
      break;
    }
#endif

    for (int restSize = argSize, curr = 0; restSize > 0; restSize--, curr++)
    {
      JSONPath& path = paths[curr];
#ifdef MYSQL_GE_1009
      int cmp = cmpJSPath(&path.p, &p, jsEg.value_type, arrayCounters);
#else
      int cmp = cmpJSPath(&path.p, &p, jsEg.value_type);
#endif
      if (cmp >= 0)
      {
        if (isModeOne)
        {
          result = true;
          break;
        }
        /* mode_all */
        if (hasFound[restSize - 1])
          continue; /* already found */
        if (--needFound == 0)
        {
          result = true;
          break;
        }
        hasFound[restSize - 1] = true;
      }
    }
  }

  if (likely(jsEg.s.error == 0))
    return result;

  isNull = true;
  return false;
}
}  // namespace funcexp
