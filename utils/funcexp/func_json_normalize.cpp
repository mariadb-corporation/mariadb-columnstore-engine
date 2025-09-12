// Include Glaze first to avoid specialization-after-instantiation
#include <glaze/glaze.hpp>
#include <string_view>

#include "functor_json.h"
#include "rowgroup.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_normalize::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_normalize::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                           execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js_ns = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";
  const std::string_view js = js_ns.unsafeStringRef();

  glz::json_t value;
  if (auto err = glz::read_json(value, js))
  {
    isNull = true;
    return "";
  }

  std::string out;
  // Write compact canonical JSON (stable ordering may vary vs server, but Glaze keeps object insertion order)
  if (auto werr = glz::write_json(value, out))
  {
    isNull = true;
    return "";
  }
  return out;
}
}  // namespace funcexp
