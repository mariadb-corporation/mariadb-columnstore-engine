// Include Glaze first to avoid specialization-after-instantiation
#include <glaze/glaze.hpp>

#include "functor_json.h"
#include "rowgroup.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_unquote::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_unquote::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                         execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  // Attempt to parse as a JSON string literal
  const std::string_view sv = js.unsafeStringRef();
  std::string out;
  if (auto err = glz::read_json(out, sv))
  {
    // Not a JSON string; return the original content
    return js.safeString("");
  }
  // Return the unescaped string (may be empty)
  return out;
}
}  // namespace funcexp
