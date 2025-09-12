#include <glaze/glaze.hpp>
#include "functor_json.h"
#include "rowgroup.h"

namespace
{

static bool numbers_equal(const glz::json_t& a, const glz::json_t& b)
{
  std::string sa, sb;
  if (auto ea = glz::write_json(a, sa))
    return false;
  if (auto eb = glz::write_json(b, sb))
    return false;
  char* enda = nullptr;
  char* endb = nullptr;
  double da = std::strtod(sa.c_str(), &enda);
  double db = std::strtod(sb.c_str(), &endb);
  return std::fabs(da - db) < 1e-12;
}

static bool overlaps(const glz::json_t& a, const glz::json_t& b)
{
  if (a.is_null() || b.is_null())
    return a.is_null() && b.is_null();
  if (a.is_boolean() && b.is_boolean())
    return a.get_boolean() == b.get_boolean();
  if (a.is_string() && b.is_string())
    return a.get_string() == b.get_string();
  if (a.is_number() && b.is_number())
    return numbers_equal(a, b);

  if (a.is_object() && b.is_object())
  {
    const auto& ao = a.get_object();
    const auto& bo = b.get_object();
    for (const auto& [k, av] : ao)
    {
      auto it = bo.find(k);
      if (it != bo.end() && overlaps(av, it->second))
        return true;
    }
    return false;
  }

  if (a.is_array() && b.is_array())
  {
    const auto& aa = a.get_array();
    const auto& bb = b.get_array();
    for (const auto& av : aa)
      for (const auto& bv : bb)
        if (overlaps(av, bv))
          return true;
    return false;
  }

  // Object vs array: true if any element overlaps object
  if (a.is_object() && b.is_array())
  {
    for (const auto& bv : b.get_array())
      if (overlaps(a, bv))
        return true;
    return false;
  }
  if (a.is_array() && b.is_object())
  {
    for (const auto& av : a.get_array())
      if (overlaps(av, b))
        return true;
    return false;
  }

  // scalar vs array: true if any element overlaps scalar
  if (a.is_array() && (b.is_string() || b.is_boolean() || b.is_number() || b.is_null()))
  {
    for (const auto& av : a.get_array())
      if (overlaps(av, b))
        return true;
    return false;
  }
  if (b.is_array() && (a.is_string() || a.is_boolean() || a.is_number() || a.is_null()))
  {
    for (const auto& bv : b.get_array())
      if (overlaps(a, bv))
        return true;
    return false;
  }

  return false;
}
}  // namespace

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_overlaps::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

bool Func_json_overlaps::getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& /*isNull*/,
                                    execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  bool n1 = false, n2 = false;
  const auto js1 = fp[0]->data()->getStrVal(row, n1);
  const auto js2 = fp[1]->data()->getStrVal(row, n2);
  if (n1 || n2)
    return false;

  glz::json_t a, b;
  if (auto e1 = glz::read_json(a, js1.unsafeStringRef()))
    return false;
  if (auto e2 = glz::read_json(b, js2.unsafeStringRef()))
    return false;

  return overlaps(a, b);
}
}  // namespace funcexp
