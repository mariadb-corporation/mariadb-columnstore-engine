#include <glaze/glaze.hpp>
#include <string_view>
#include <memory>

#include "functor_json.h"
#include "rowgroup.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_equals::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_equals::getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js1_ns = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  const auto js2_ns = fp[1]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  const std::string_view js1 = js1_ns.unsafeStringRef();
  const std::string_view js2 = js2_ns.unsafeStringRef();

  glz::json_t v1, v2;
  if (auto e1 = glz::read_json(v1, js1))
  {
    isNull = true;
    return false;
  }
  if (auto e2 = glz::read_json(v2, js2))
  {
    isNull = true;
    return false;
  }

  // Compare canonical serialized representations to determine equality
  std::string s1, s2;
  if (auto e = glz::write_json(v1, s1))
  {
    isNull = true;
    return false;
  }
  if (auto e = glz::write_json(v2, s2))
  {
    isNull = true;
    return false;
  }
  return s1 == s2;
}
}  // namespace funcexp
