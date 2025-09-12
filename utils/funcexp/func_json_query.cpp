// Include Glaze first
#include <glaze/glaze.hpp>
#include <cctype>

#include "functor_json.h"

#include "rowgroup.h"
#include "glaze_path.h"

namespace funcexp
{

execplan::CalpontSystemCatalog::ColType Func_json_query::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_query::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                       execplan::CalpontSystemCatalog::ColType& /*type*/)
{
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

  // Prefer the first complex (object/array) match; otherwise NULL
  const glz::json_t* selected = nullptr;
  for (const auto* m : matches)
  {
    if (m->is_object() || m->is_array())
    {
      selected = m;
      break;
    }
  }
  if (!selected)
  {
    isNull = true;
    return "";
  }

  std::string out;
  if (auto w = glz::write_json(*selected, out))
  {
    isNull = true;
    return "";
  }
  return out;
}

}  // namespace funcexp
