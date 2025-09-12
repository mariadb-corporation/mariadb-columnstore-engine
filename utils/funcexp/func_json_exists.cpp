// Include Glaze first
#include <glaze/glaze.hpp>

#include "functor_json.h"
#include "rowgroup.h"
#include "glaze_path.h"

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_exists::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_exists::getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  glz::json_t doc;
  if (auto e = glz::read_json(doc, js.unsafeStringRef()))
  {
    isNull = true;
    return false;
  }

  bool pNull = false;
  const auto path_ns = fp[1]->data()->getStrVal(row, pNull);
  if (pNull)
  {
    isNull = true;
    return false;
  }

  std::vector<const glz::json_t*> matches;
  if (!glaze_path::find_matches(doc, path_ns.unsafeStringRef(), matches))
  {
    isNull = true;
    return false;
  }
  return !matches.empty();
}
}  // namespace funcexp
