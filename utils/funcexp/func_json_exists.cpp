// Include Glaze first
#include <glaze/glaze.hpp>

#include "functor_json.h"
#include "rowgroup.h"
#include "glaze_path.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_exists::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_exists::getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  glz::json_t doc;
  if (auto e = glz::read_json(doc, js.unsafeStringRef()))
  {
    isNull = true;
    return false;
  }

  bool pNull = false;
  const auto path_ns = fp[1]->data()->getStrVal(row, pNull);
  if (pNull)
  {
    isNull = true;
    return false;
  }

  std::vector<const glz::json_t*> matches;
  if (!glaze_path::find_matches(doc, path_ns.unsafeStringRef(), matches))
  {
    isNull = true;
    return false;
  }
  if (!matches.empty())
    return true;

  // Special-case: allow indexing into strings for existence checks, e.g. $.key1[0]
  // If the direct match was empty, check whether the last step is an Index on a string parent
  std::vector<glaze_path::Step> steps;
  if (!glaze_path::parse(path_ns.unsafeStringRef(), steps) || steps.empty())
    return false;
  if (steps.size() < 2)
    return false;

  glaze_path::Step last = steps.back();
  if (last.kind != glaze_path::StepKind::Index)
    return false;
  steps.pop_back();

  std::vector<const glz::json_t*> parents;
  glaze_path::match_impl(doc, steps, 0, parents);
  for (const auto* p : parents)
  {
    if (p && p->is_string())
    {
      int64_t len = static_cast<int64_t>(p->get_string().size());
      int idx = last.index;
      if (last.from_end)
        idx = static_cast<int>(len) - 1 - idx;
      if (idx < 0)
        idx = static_cast<int>(len) + idx;
      if (idx >= 0 && idx < len)
        return true;
    }
  }
  return false;
}
}  // namespace funcexp
