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

  Func_json_contains_path_state state(fp);

  const int argSize = fp.size() - 2;

  if (!state.isModeParsed)
  {
    if (!state.isModeConst)
      state.isModeConst = (dynamic_cast<ConstantColumn*>(fp[1]->data()) != nullptr);

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

    state.isModeOne = (mode == "one");
    state.isModeParsed = state.isModeConst;
  }

  if (state.paths.size() == 0)
    state.hasFound.assign(argSize, false);

  for (size_t i = 2; i < fp.size(); i++)
  {
    JSONPath& path = state.paths[i - 2];

    if (!path.parsed)
    {
      if (parseJSPath(path, row, fp[i]))
      {
        isNull = true;
        return false;
      }
    }
    // path is parsed.
    initJSEngine(state.jsEg, getCharset(fp[0]), js_ns);

    int jsErr = 0;

    if (locateJSPath(state.jsEg, path, &jsErr))
    {
      if (jsErr)
      {
        isNull = true;
        return false;
      }
      if (!state.isModeOne)
      {
        return false;
      }
    }
    else
    {
      if (state.isModeOne)
      {
        return true;
      }
    }
  }
  if (state.isModeOne)
  {
    return false; // none at all.
  }
  return true;

}
}  // namespace funcexp
