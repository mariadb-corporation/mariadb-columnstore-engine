// Include Glaze first to avoid specialization-after-instantiation
#include <glaze/glaze.hpp>
#include "functor_json.h"

#include <string>
#include "rowgroup.h"

static constexpr int LOCAL_TAB_SIZE_LIMIT = 8;

// Glaze JSON
#include <glaze/glaze.hpp>

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_format::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_format::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                        execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  const auto& js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  int tabSize = 4;

  if (fmt == DETAILED)
  {
    if (fp.size() > 1)
    {
      tabSize = fp[1]->data()->getIntVal(row, isNull);
      if (isNull)
        return "";

      if (tabSize < 0)
        tabSize = 0;
      else if (tabSize > LOCAL_TAB_SIZE_LIMIT)
        tabSize = LOCAL_TAB_SIZE_LIMIT;
    }
  }

  const std::string_view sv{js.unsafeStringRef().data(), js.unsafeStringRef().size()};
  glz::json_t value;
  if (auto err = glz::read_json(value, sv))
  {
    isNull = true;
    return "";
  }

  std::string out;
  // Current Glaze in dependency offers two-argument write_json; use that and check for errors
  if (auto werr = writeJson(value, out))
  {
    isNull = true;
    return "";
  }

  isNull = false;
  return out;
}
}  // namespace funcexp
