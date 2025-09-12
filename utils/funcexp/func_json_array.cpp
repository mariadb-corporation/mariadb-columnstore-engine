#include <glaze/glaze.hpp>
#include <string>

#include "functor_json.h"
#include "rowgroup.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_array::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType)
{
  return fp.size() > 0 ? fp[0]->data()->resultType() : resultType;
}

std::string Func_json_array::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                       execplan::CalpontSystemCatalog::ColType& type)
{
  if (fp.size() == 0)
    return "[]";

  glz::json_t arr;
  auto& a = arr.get_array();
  a.reserve(fp.size());

  for (size_t i = 0; i < fp.size(); ++i)
  {
    bool argNull = false;
    const auto ns = fp[i]->data()->getStrVal(row, argNull);
    if (argNull)
    {
      a.emplace_back();  // null
      continue;
    }

    auto& valType = fp[i]->data()->resultType();
    if (isCharType(valType.colDataType))
    {
      a.emplace_back(ns.safeString(""));
      continue;
    }

    glz::json_t v;
    if (auto e = glz::read_json(v, ns.unsafeStringRef()))
    {
      a.emplace_back(ns.safeString(""));
    }
    else
    {
      a.emplace_back(std::move(v));
    }
  }

  std::string out;
  if (auto w = glz::write_json(arr, out))
  {
    isNull = true;
    return "";
  }
  return out;
}

}  // namespace funcexp
