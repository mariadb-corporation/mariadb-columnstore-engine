#include <glaze/glaze.hpp>

#include "functor_json.h"
#include "rowgroup.h"

#include "glaze_path.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_insert::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_insert::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                        execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  glz::json_t doc;
  if (auto e = glz::read_json(doc, js.unsafeStringRef()))
  {
    isNull = true;
    return "";
  }

  const bool isInsertMode = (mode == INSERT) || (mode == SET);
  const bool isReplaceMode = (mode == REPLACE) || (mode == SET);

  // process pairs: path, value
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

    std::vector<funcexp::glaze_path::Step> steps;
    if (!funcexp::glaze_path::parse(p_ns.unsafeStringRef(), steps))
    {
      isNull = true;
      return "";
    }

    // Only constrain wildcards on parent steps; last step may be wildcard
    bool parent_has_illegal = false;
    for (size_t si = 0; si + 1 < steps.size(); ++si)
    {
      const auto& s = steps[si];
      if (s.kind != funcexp::glaze_path::StepKind::Key && s.kind != funcexp::glaze_path::StepKind::Index)
        parent_has_illegal = true;
    }
    if (parent_has_illegal)
    {
      isNull = true;
      return "";
    }

    if (steps.empty())
    {
      isNull = true;
      return "";
    }

    funcexp::glaze_path::Step last = steps.back();
    steps.pop_back();

    // Find all parent matches (wildcards/recursive supported)
    std::vector<glz::json_t*> parents;
    funcexp::glaze_path::find_matches_mutable_steps(doc, steps, parents);

    if (parents.empty())
    {
      // If no parents matched, skip this pair (no-op)
      continue;
    }

    for (auto* cur : parents)
    {
      // Apply at last step for each matched parent
      if (last.kind == funcexp::glaze_path::StepKind::Key)
      {
        if (!cur->is_object())
        {
          isNull = true;
          return "";
        }
        auto& obj = cur->get_object();
        auto it = obj.find(last.key);
        bool exists = (it != obj.end());
        if (isReplaceMode && exists)
        {
          it->second = value;
        }
        else if (isInsertMode && !exists)
        {
          obj.emplace(last.key, value);
        }
        // SET semantics covered by the above
      }
      else if (last.kind == funcexp::glaze_path::StepKind::Index)
      {
        if (!cur->is_array())
        {
          // If parent is not array, wrap into array first to permit insert
          glz::json_t arr;
          arr.get_array().push_back(*cur);
          *cur = std::move(arr);
        }
        auto& arr = cur->get_array();
        int idx = last.index;
        if (idx < 0)
          idx = static_cast<int>(arr.size()) + idx;

        if (isReplaceMode && idx >= 0 && static_cast<size_t>(idx) < arr.size())
        {
          arr[static_cast<size_t>(idx)] = value;
        }
        else if (isInsertMode)
        {
          // insert at index or append if index == size
          if (idx < 0 || static_cast<size_t>(idx) > arr.size())
          {
            isNull = true;
            return "";
          }
          arr.insert(arr.begin() + idx, value);
        }
      }
      else if (last.kind == funcexp::glaze_path::StepKind::KeyWildcard)
      {
        // Apply to all keys in object for REPLACE/SET; INSERT has no effect
        if (!cur->is_object())
          continue;
        if (isReplaceMode)
        {
          auto& obj = cur->get_object();
          for (auto& [k, v] : obj)
            v = value;
        }
      }
      else if (last.kind == funcexp::glaze_path::StepKind::IndexWildcard)
      {
        // For arrays: REPLACE/SET replace all elements; INSERT appends value once
        if (!cur->is_array())
        {
          // For non-array parent, wrap then proceed as append/replace
          glz::json_t arr;
          arr.get_array().push_back(*cur);
          *cur = std::move(arr);
        }
        auto& arr = cur->get_array();
        if (isReplaceMode)
        {
          for (auto& el : arr)
            el = value;
        }
        else if (isInsertMode)
        {
          arr.push_back(value);
        }
      }
    }
  }

  std::string out;
  if (auto w = glz::write_json(doc, out))
  {
    isNull = true;
    return "";
  }
  return out;
}
}  // namespace funcexp
