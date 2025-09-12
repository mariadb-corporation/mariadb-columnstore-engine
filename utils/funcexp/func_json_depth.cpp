#include <glaze/glaze.hpp>
#include "functor_json.h"
#include <functional>
#include <algorithm>

#include "rowgroup.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_depth::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

int64_t Func_json_depth::getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& /*op_ct*/)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return 0;

  const std::string_view sv{js.unsafeStringRef().data(), js.unsafeStringRef().size()};
  glz::json_t value;
  if (auto err = glz::read_json(value, sv))
  {
    isNull = true;
    return 0;
  }

  // Compute depth: scalars have depth 1; arrays/objects are 1 + max(child depth)
  std::function<int64_t(const glz::json_t&)> compute_depth = [&](const glz::json_t& v) -> int64_t
  {
    if (v.is_object())
    {
      int64_t max_child = 0;
      for (const auto& [k, child] : v.get_object())
      {
        max_child = std::max(max_child, compute_depth(child));
      }
      return 1 + max_child;
    }
    if (v.is_array())
    {
      int64_t max_child = 0;
      for (const auto& child : v.get_array())
      {
        max_child = std::max(max_child, compute_depth(child));
      }
      return 1 + max_child;
    }
    return 1;  // scalars/null
  };

  return compute_depth(value);
}
}  // namespace funcexp
