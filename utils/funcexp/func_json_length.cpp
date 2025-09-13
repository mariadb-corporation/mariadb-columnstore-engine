
#include <glaze/glaze.hpp>
#include "functor_json.h"
#include "glaze_path.h"

#include "rowgroup.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_length::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

int64_t Func_json_length::getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                    execplan::CalpontSystemCatalog::ColType& /*op_ct*/)
{
  const auto& js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return 0;

  // If a JSONPath is provided, evaluate the length of the node at that path
  const bool has_path = (fp.size() > 1);

  const std::string_view sv{js.unsafeStringRef().data(), js.unsafeStringRef().size()};
  glz::json_t value;
  if (auto err = glz::read_json(value, sv))
  {
    isNull = true;
    return 0;
  }

  const glz::json_t* target = &value;
  if (has_path)
  {
    bool pNull = false;
    const auto path_ns = fp[1]->data()->getStrVal(row, pNull);
    if (pNull)
    {
      isNull = true;
      return 0;
    }
    std::vector<const glz::json_t*> matches;
    if (!funcexp::glaze_path::find_matches(value, path_ns.unsafeStringRef(), matches) || matches.empty())
    {
      isNull = true;
      return 0;
    }
    target = matches.front();
  }

  if (target->is_array())
    return static_cast<int64_t>(target->get_array().size());
  if (target->is_object())
    return static_cast<int64_t>(target->get_object().size());
  // With a path, scalars/null should yield NULL; without a path, return 1
  if (has_path)
  {
    isNull = true;
    return 0;
  }
  return 1;
}
}  // namespace funcexp
