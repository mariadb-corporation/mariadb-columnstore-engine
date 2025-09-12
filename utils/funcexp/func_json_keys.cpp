// Include Glaze first
#include <glaze/glaze.hpp>

#include "functor_json.h"
#include "rowgroup.h"
#include "glaze_path.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_keys::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_keys::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
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

  const glz::json_t* node = &doc;
  if (fp.size() > 1)
  {
    bool pNull = false;
    const auto p = fp[1]->data()->getStrVal(row, pNull);
    if (pNull)
    {
      isNull = true;
      return "";
    }
    std::vector<const glz::json_t*> matches;
    if (!glaze_path::find_matches(doc, p.unsafeStringRef(), matches) || matches.empty())
    {
      isNull = true;
      return "";
    }
    // Choose the first object match; otherwise NULL
    const glz::json_t* first_obj = nullptr;
    for (const auto* m : matches)
    {
      if (m->is_object())
      {
        first_obj = m;
        break;
      }
    }
    if (!first_obj)
    {
      isNull = true;
      return "";
    }
    node = first_obj;
  }

  if (!node->is_object())
  {
    isNull = true;
    return "";
  }

  std::vector<std::string> keys;
  keys.reserve(node->get_object().size());
  for (const auto& [k, v] : node->get_object())
  {
    // Avoid duplicates by checking recent entries (object keys are unique anyway)
    keys.push_back(k);
  }

  glz::json_t out;
  // Ensure variant holds an array before accessing it
  out = std::vector<glz::json_t>{};
  auto& arr = out.get_array();
  arr.reserve(keys.size());
  for (const auto& k : keys)
  {
    // push JSON string values
    arr.emplace_back(std::string{k});
  }

  std::string ret;
  if (auto w = writeJson(out, ret))
  {
    isNull = true;
    return "";
  }
  return ret;
}
}  // namespace funcexp
