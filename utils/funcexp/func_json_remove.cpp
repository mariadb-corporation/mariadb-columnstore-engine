// Glaze-based implementation
#include <glaze/glaze.hpp>

#include "functor_json.h"
#include "glaze_path.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_remove::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_remove::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                        execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js_ns = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  glz::json_t doc;
  if (auto e = glz::read_json(doc, js_ns.unsafeStringRef()))
  {
    isNull = true;
    return "";
  }

  // For each path, remove target (wildcards/recursive supported on parent)
  for (size_t i = 1; i < fp.size(); ++i)
  {
    bool pNull = false;
    const auto p_ns = fp[i]->data()->getStrVal(row, pNull);
    if (pNull)
    {
      isNull = true;
      return "";
    }

    std::vector<funcexp::glaze_path::Step> steps;
    if (!funcexp::glaze_path::parse(p_ns.unsafeStringRef(), steps))
    {
      isNull = true;
      return "";
    }
    if (steps.empty())
      continue;
    auto last = steps.back();
    steps.pop_back();

    std::vector<glz::json_t*> parents;
    funcexp::glaze_path::find_matches_mutable_steps(doc, steps, parents);

    if (parents.empty())
      continue;  // nothing to remove for this path

    for (auto* parent : parents)
    {
      if (last.kind == funcexp::glaze_path::StepKind::Key)
      {
        if (!parent->is_object())
          continue;
        parent->get_object().erase(last.key);
      }
      else if (last.kind == funcexp::glaze_path::StepKind::Index)
      {
        if (!parent->is_array())
          continue;
        auto& arr = parent->get_array();
        int idx = last.index;
        if (idx < 0)
          idx = static_cast<int>(arr.size()) + idx;
        if (idx >= 0 && static_cast<size_t>(idx) < arr.size())
          arr.erase(arr.begin() + idx);
      }
      else
      {
        // If last is wildcard, remove all children accordingly
        if (parent->is_object() && (last.kind == funcexp::glaze_path::StepKind::KeyWildcard))
        {
          parent->get_object().clear();
        }
        else if (parent->is_array() && (last.kind == funcexp::glaze_path::StepKind::IndexWildcard))
        {
          parent->get_array().clear();
        }
      }
    }
  }

  std::string out;
  if (auto w = writeJson(doc, out))
  {
    isNull = true;
    return "";
  }
  return out;
}
}  // namespace funcexp
