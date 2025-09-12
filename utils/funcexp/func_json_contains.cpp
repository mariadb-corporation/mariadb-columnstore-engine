#include <glaze/glaze.hpp>
#include "functor_json.h"
#include "rowgroup.h"
#include "glaze_path.h"

namespace
{
static bool contains_json(const glz::json_t& doc, const glz::json_t& val)
{
  if (doc.is_object())
  {
    if (!val.is_object())
      return false;
    const auto& D = doc.get_object();
    const auto& V = val.get_object();
    for (const auto& [k, vv] : V)
    {
      auto it = D.find(k);
      if (it == D.end())
        return false;
      if (!contains_json(it->second, vv))
        return false;
    }
    return true;
  }
  if (doc.is_array())
  {
    const auto& A = doc.get_array();
    if (val.is_array())
    {
      // Every element in val must be contained by some element in doc array
      for (const auto& vv : val.get_array())
      {
        bool any = false;
        for (const auto& dv : A)
        {
          if (contains_json(dv, vv))
          {
            any = true;
            break;
          }
        }
        if (!any)
          return false;
      }
      return true;
    }
    // val is not array: any element contains val
    for (const auto& dv : A)
      if (contains_json(dv, val))
        return true;
    return false;
  }
  if (doc.is_string())
  {
    return val.is_string() && doc.get_string() == val.get_string();
  }
  if (doc.is_boolean())
  {
    return val.is_boolean() && doc.get_boolean() == val.get_boolean();
  }
  if (doc.is_null())
  {
    return val.is_null();
  }
  if (doc.is_number() && val.is_number())
  {
    std::string sd, sv;
    if (auto ed = glz::write_json(doc, sd))
      return false;
    if (auto ev = glz::write_json(val, sv))
      return false;
    char* endd = nullptr;
    char* endv = nullptr;
    double dd = std::strtod(sd.c_str(), &endd);
    double dv = std::strtod(sv.c_str(), &endv);
    return std::fabs(dd - dv) < 1e-12;
  }
  return false;
}
}  // namespace

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_contains::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_contains::getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                    execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  bool isNullJS = false, isNullVal = false;
  const auto js_ns = fp[0]->data()->getStrVal(row, isNullJS);
  const auto val_ns = fp[1]->data()->getStrVal(row, isNullVal);
  if (isNullJS || isNullVal)
  {
    isNull = true;
    return false;
  }

  glz::json_t doc;
  if (auto e = glz::read_json(doc, js_ns.unsafeStringRef()))
  {
    isNull = true;
    return false;
  }
  glz::json_t needle;
  if (auto e2 = glz::read_json(needle, val_ns.unsafeStringRef()))
  {
    isNull = true;
    return false;
  }

  // Optional path: use first match; if none, return NULL (match prior behavior)
  if (fp.size() > 2)
  {
    bool pNull = false;
    const auto p = fp[2]->data()->getStrVal(row, pNull);
    if (pNull)
    {
      isNull = true;
      return false;
    }
    std::vector<const glz::json_t*> matches;
    if (!glaze_path::find_matches(doc, p.unsafeStringRef(), matches) || matches.empty())
    {
      isNull = true;
      return false;
    }
    doc = *matches.front();
  }

  bool result = contains_json(doc, needle);
  return result;
}
}  // namespace funcexp
