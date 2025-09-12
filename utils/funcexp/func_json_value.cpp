#include <glaze/glaze.hpp>
#include <cctype>

#include "functor_json.h"
#include "rowgroup.h"

#include "glaze_path.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_value::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}
std::string Func_json_value::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                       execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  // Expect JSON doc and a single path argument
  bool nullDoc = false, nullPath = false;
  const auto js = fp[0]->data()->getStrVal(row, nullDoc);
  const auto path_ns = fp[1]->data()->getStrVal(row, nullPath);
  if (nullDoc || nullPath)
  {
    isNull = true;
    return "";
  }

  glz::json_t doc;
  if (auto e = glz::read_json(doc, js.unsafeStringRef()))
  {
    isNull = true;
    return "";
  }

  std::vector<const glz::json_t*> matches;
  if (!glaze_path::find_matches(doc, path_ns.unsafeStringRef(), matches) || matches.empty())
  {
    isNull = true;
    return "";
  }

  const glz::json_t& value = *matches.front();

  // Only scalars produce a result
  if (value.is_string())
  {
    // return raw unescaped string
    return value.get_string();
  }
  if (value.is_number())
  {
    std::string out;
    if (auto w = writeJson(value, out))
    {
      isNull = true;
      return "";
    }
    return out;
  }
  if (value.is_boolean())
  {
    return value.get_boolean() ? "1" : "0";
  }

  isNull = true;
  return "";
}
}  // namespace funcexp
