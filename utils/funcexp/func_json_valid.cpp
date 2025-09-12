#include <glaze/glaze.hpp>
#include <string_view>
#include "functor_json.h"
#include "rowgroup.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_valid::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_valid::getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                 execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  // Validate by attempting to parse into a dynamic Glaze JSON value
  // Any parse error indicates invalid JSON
  const std::string_view sv{js.unsafeStringRef().data(), js.unsafeStringRef().size()};
  glz::json_t value;  // dynamic JSON value
  auto err = glz::read_json(value, sv);
  return !err;  // true if parsing succeeded
}
}  // namespace funcexp
