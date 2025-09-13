#include <string_view>
#include <glaze/glaze.hpp>
#include "functor_json.h"

#include "rowgroup.h"
#include "mcs_datatype.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_object::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType)
{
  return fp.size() > 0 ? fp[0]->data()->resultType() : resultType;
}

std::string Func_json_object::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                        execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  if (fp.size() == 0)
    return "{}";

  glz::json_t obj = glz::json_t::object_t{};
  auto& o = obj.get_object();

  auto add_pair = [&](size_t keyIdx, size_t valIdx) -> bool
  {
    bool keyNull = false, valNull = false;
    const auto key_ns = fp[keyIdx]->data()->getStrVal(row, keyNull);
    const auto val_ns = fp[valIdx]->data()->getStrVal(row, valNull);
    std::string key = keyNull ? std::string("") : key_ns.safeString("");

    if (valNull)
    {
      o[key] = glz::json_t{};  // null
      return true;
    }

    // Check value type to decide quoting
    auto& valType = fp[valIdx]->data()->resultType();
    if (isCharType(valType.colDataType))
    {
      o[key] = glz::json_t{val_ns.safeString("")};
      return true;
    }

    // Try parse as JSON; fallback to string if parsing fails
    glz::json_t v;
    if (auto e = glz::read_json(v, val_ns.unsafeStringRef()))
    {
      o[key] = glz::json_t{val_ns.safeString("")};
    }
    else
    {
      o[key] = std::move(v);
    }
    return true;
  };

  if (!add_pair(0, 1))
  {
    isNull = true;
    return "";
  }
  for (size_t i = 2; i + 1 < fp.size(); i += 2)
  {
    if (!add_pair(i, i + 1))
    {
      isNull = true;
      return "";
    }
  }

  std::string out;
  if (auto w = writeJson(obj, out))
  {
    isNull = true;
    return "";
  }
  return out;
}
}  // namespace funcexp
