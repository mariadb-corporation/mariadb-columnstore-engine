#include <glaze/glaze.hpp>

#include "functor_json.h"
#include "rowgroup.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_type::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_type::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  const std::string_view sv{js.unsafeStringRef().data(), js.unsafeStringRef().size()};
  glz::json_t value;
  if (auto err = glz::read_json(value, sv))
  {
    isNull = true;
    return "";
  }

  if (value.is_object())
    return "OBJECT";
  if (value.is_array())
    return "ARRAY";
  if (value.is_string())
    return "STRING";
  if (value.is_number())
  {
    // Determine integer vs floating by canonical serialization
    std::string tmp;
    if (auto werr = glz::write_json(value, tmp))
    {
      isNull = true;
      return "";
    }
    for (char ch : tmp)
    {
      if (ch == '.' || ch == 'e' || ch == 'E')
        return "DOUBLE";
    }
    return "INTEGER";
  }
  if (value.is_boolean())
    return "BOOLEAN";
  return "NULL";
}
}  // namespace funcexp
