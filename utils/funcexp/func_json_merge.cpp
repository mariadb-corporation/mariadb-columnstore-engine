#include <glaze/glaze.hpp>

#include "functor_json.h"
#include "rowgroup.h"

namespace
{
// Merge semantics similar to JSON_MERGE_PRESERVE
static void merge_in_place(glz::json_t& a, const glz::json_t& b)
{
  if (a.is_object() && b.is_object())
  {
    auto& ao = a.get_object();
    const auto& bo = b.get_object();
    for (const auto& [k, bv] : bo)
    {
      auto it = ao.find(k);
      if (it == ao.end())
      {
        ao.emplace(k, bv);
      }
      else
      {
        merge_in_place(it->second, bv);
      }
    }
    return;
  }

  // Anything else becomes an array concatenation
  glz::json_t arr = std::vector<glz::json_t>{};
  arr.get_array().reserve((a.is_array() ? a.get_array().size() : 1) +
                          (b.is_array() ? b.get_array().size() : 1));
  if (a.is_array())
  {
    for (const auto& v : a.get_array())
      arr.get_array().push_back(v);
  }
  else
  {
    arr.get_array().push_back(a);
  }

  if (b.is_array())
  {
    for (const auto& v : b.get_array())
      arr.get_array().push_back(v);
  }
  else
  {
    arr.get_array().push_back(b);
  }

  a = std::move(arr);
}
}  // namespace

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_merge::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_merge::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                       execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  glz::json_t acc;
  if (auto e = glz::read_json(acc, js.unsafeStringRef()))
  {
    isNull = true;
    return "";
  }

  for (size_t i = 1; i < fp.size(); ++i)
  {
    const auto js2 = fp[i]->data()->getStrVal(row, isNull);
    if (isNull)
    {
      return "";
    }
    glz::json_t rhs;
    if (auto e2 = glz::read_json(rhs, js2.unsafeStringRef()))
    {
      isNull = true;
      return "";
    }
    merge_in_place(acc, rhs);
  }

  std::string out;
  if (auto w = writeJson(acc, out))
  {
    isNull = true;
    return "";
  }
  isNull = false;
  return out;
}
}  // namespace funcexp
