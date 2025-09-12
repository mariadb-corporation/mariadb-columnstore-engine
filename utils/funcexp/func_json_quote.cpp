// Include Glaze first to avoid specialization-after-instantiation
#include <glaze/glaze.hpp>
#include <string_view>

#include "functor_json.h"
#include "rowgroup.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_quote::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_quote::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                       execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull || !isCharType(fp[0]->data()->resultType().colDataType))
  {
    isNull = true;
    return "";
  }

  // Use Glaze to emit a JSON-escaped, quoted string
  const std::string_view sv = js.unsafeStringRef();
  std::string out;
  if (auto err = glz::write_json(sv, out))
  {
    isNull = true;
    return "";
  }
  return out;
}
}  // namespace funcexp
