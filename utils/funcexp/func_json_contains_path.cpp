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
    FunctionParm& fp, [[maybe_unused]] CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_contains_path::getBoolVal(Row& row, FunctionParm& fp, bool& isNull,
                                         [[maybe_unused]] CalpontSystemCatalog::ColType& type)
{
  const auto& js_ns = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  const std::string_view js = js_ns.unsafeStringRef();

#if MYSQL_VERSION_ID >= 100900
  int arrayCounters[JSON_DEPTH_LIMIT];
  bool hasNegPath = false;
#endif
  const int argSize = fp.size() - 2;

  if (!isModeParsed)
  {
    if (!isModeConst)
      isModeConst = (dynamic_cast<ConstantColumn*>(fp[1]->data()) != nullptr);

    auto mode_ns = fp[1]->data()->getStrVal(row, isNull);
    if (isNull)
      return false;
    std::string mode = mode_ns.unsafeStringRef();

    transform(mode.begin(), mode.end(), mode.begin(), ::tolower);
    if (mode != "one" && mode != "all")
    {
      isNull = true;
      return false;
    }

    isModeOne = (mode == "one");
    isModeParsed = isModeConst;
  }

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
#if MYSQL_VERSION_ID >= 100900
      hasNegPath |= path.p.types_used & JSON_PATH_NEGATIVE_INDEX;
#endif
    }
  }

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
#if MYSQL_VERSION_ID >= 100900
#if MYSQL_VERSION_ID >= 120200
    json_path_step_t *last_step= reinterpret_cast<json_path_step_t*>
                                  (mem_root_dynamic_array_get_val(&p.steps,
                                                                  p.last_step_idx));
    if (hasNegPath && jsEg.value_type == JSON_VALUE_ARRAY &&
        json_skip_array_and_count(&jsEg, arrayCounters + (last_step - reinterpret_cast<json_path_step_t*>(p.steps.buffer))))
#else
    if (hasNegPath && jsEg.value_type == JSON_VALUE_ARRAY &&
        json_skip_array_and_count(&jsEg, arrayCounters + (p.last_step - p.steps)))
    {
      result = true;
      break;
    }
#endif
#endif

    for (int restSize = argSize, curr = 0; restSize > 0; restSize--, curr++)
    {
      JSONPath& path = paths[curr];
#if MYSQL_VERSION_ID >= 100900
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
