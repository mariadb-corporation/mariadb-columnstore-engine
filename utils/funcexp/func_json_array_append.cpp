#include <glaze/glaze.hpp>

#include "functor_json.h"
#include "rowgroup.h"
#include "glaze_path.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_array_append::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_array_append::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js_ns = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  glz::json_t doc;
  if (auto e = glz::read_json(doc, js_ns.unsafeStringRef()))
  {
    isNull = true;
    return "";
  }

  for (size_t i = 1; i + 1 < fp.size(); i += 2)
  {
    bool pNull = false, vNull = false;
    const auto p_ns = fp[i]->data()->getStrVal(row, pNull);
    const auto v_ns = fp[i + 1]->data()->getStrVal(row, vNull);
    if (pNull || vNull)
    {
      isNull = true;
      return "";
    }

    glz::json_t value;
    if (auto ev = glz::read_json(value, v_ns.unsafeStringRef()))
    {
      isNull = true;
      return "";
    }

    std::vector<glz::json_t*> nodes;
    if (!glaze_path::find_matches_mutable(doc, p_ns.unsafeStringRef(), nodes))
    {
      isNull = true;
      return "";
    }

    for (auto* node : nodes)
    {
      if (node->is_array())
      {
        node->get_array().push_back(value);
      }
      else
      {
        glz::json_t arr;
        arr.get_array().push_back(*node);
        arr.get_array().push_back(value);
        *node = std::move(arr);
      }
    }
  }

  std::string out;
  if (auto w = glz::write_json(doc, out))
  {
    isNull = true;
    return "";
  }
  return out;
}

}  // namespace funcexp
