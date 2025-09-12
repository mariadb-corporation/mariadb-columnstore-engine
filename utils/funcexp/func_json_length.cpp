
#include <glaze/glaze.hpp>
#include "functor_json.h"

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

  // Path-based form will be migrated later; return NULL for now when path is provided
  if (fp.size() > 1)
  {
    isNull = true;
    return 0;
  }

  const std::string_view sv{js.unsafeStringRef().data(), js.unsafeStringRef().size()};
  glz::json_t value;
  if (auto err = glz::read_json(value, sv))
  {
    isNull = true;
    return 0;
  }

  if (value.is_array())
    return static_cast<int64_t>(value.get_array().size());
  if (value.is_object())
    return static_cast<int64_t>(value.get_object().size());
  // Scalars and null count as length 1
  return 1;
}
}  // namespace funcexp
