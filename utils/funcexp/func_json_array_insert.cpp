#include <glaze/glaze.hpp>

#include "functor_json.h"
#include "rowgroup.h"
#include "glaze_path.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_array_insert::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_array_insert::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
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

  for (size_t i = 1; i + 1 < fp.size(); i += 2)
  {
    bool pNull = false, vNull = false;
    const auto p_ns = fp[i]->data()->getStrVal(row, pNull);
    const auto v_ns = fp[i + 1]->data()->getStrVal(row, vNull);
    if (pNull || vNull)
    {
      isNull = true;
      return "";
    }

    glz::json_t value;
    if (auto ev = glz::read_json(value, v_ns.unsafeStringRef()))
    {
      isNull = true;
      return "";
    }

    // Parse path and require it ends with an array index step
    std::vector<funcexp::glaze_path::Step> steps;
    if (!funcexp::glaze_path::parse(p_ns.unsafeStringRef(), steps))
    {
      isNull = true;
      return "";
    }
    if (steps.empty() || steps.back().kind != funcexp::glaze_path::StepKind::Index)
    {
      isNull = true;
      return "";
    }

    // Split into parent and index
    auto last = steps.back();
    steps.pop_back();

    std::vector<glz::json_t*> parents;
    funcexp::glaze_path::find_matches_mutable_steps(doc, steps, parents);

    for (auto* parent : parents)
    {
      // Ensure parent is an array, or wrap into array first
      if (!parent->is_array())
      {
        glz::json_t arr;
        // Initialize as array variant before using get_array()
        arr = std::vector<glz::json_t>{};
        arr.get_array().push_back(*parent);
        *parent = std::move(arr);
      }
      auto& arr = parent->get_array();
      int idx = last.index;
      if (idx < 0)
        idx = static_cast<int>(arr.size()) + idx;
      if (idx < 0 || static_cast<size_t>(idx) > arr.size())
      {
        isNull = true;
        return "";
      }
      arr.insert(arr.begin() + idx, value);
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
