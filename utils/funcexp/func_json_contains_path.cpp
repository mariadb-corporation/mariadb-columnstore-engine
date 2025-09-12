#include <glaze/glaze.hpp>
#include <algorithm>

#include "functor_json.h"
#include "constantcolumn.h"
#include "rowgroup.h"
#include "glaze_path.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_contains_path::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_contains_path::getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                         execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto& js_ns = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  glz::json_t doc;
  if (auto e = glz::read_json(doc, js_ns.unsafeStringRef()))
  {
    isNull = true;
    return false;
  }

  // Parse mode once (const optimization preserved)
  if (!isModeParsed)
  {
    if (!isModeConst)
      isModeConst = (dynamic_cast<execplan::ConstantColumn*>(fp[1]->data()) != nullptr);

    auto mode_ns = fp[1]->data()->getStrVal(row, isNull);
    if (isNull)
      return false;
    std::string mode = mode_ns.safeString("");

    transform(mode.begin(), mode.end(), mode.begin(), ::tolower);
    if (mode != "one" && mode != "all")
    {
      isNull = true;
      return false;
    }

    isModeOne = (mode == "one");
    isModeParsed = isModeConst;
  }

  const int argSize = fp.size() - 2;
  if (argSize <= 0)
  {
    isNull = true;
    return false;
  }

  if (isModeOne)
  {
    // True if any path has at least one match
    for (size_t i = 2; i < fp.size(); ++i)
    {
      bool pNull = false;
      const auto p = fp[i]->data()->getStrVal(row, pNull);
      if (pNull)
      {
        isNull = true;
        return false;
      }
      std::vector<const glz::json_t*> matches;
      if (!glaze_path::find_matches(doc, p.unsafeStringRef(), matches))
      {
        isNull = true;
        return false;  // path parse error
      }
      if (!matches.empty())
        return true;
    }
    return false;
  }
  else
  {
    // True only if all paths have at least one match
    for (size_t i = 2; i < fp.size(); ++i)
    {
      bool pNull = false;
      const auto p = fp[i]->data()->getStrVal(row, pNull);
      if (pNull)
      {
        isNull = true;
        return false;
      }
      std::vector<const glz::json_t*> matches;
      if (!glaze_path::find_matches(doc, p.unsafeStringRef(), matches))
      {
        isNull = true;
        return false;  // path parse error
      }
      if (matches.empty())
        return false;
    }
    return true;
  }
}
}  // namespace funcexp
