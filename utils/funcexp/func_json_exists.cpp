#include "functor_json.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
#include "rowgroup.h"
using namespace execplan;
using namespace rowgroup;

#include "dataconvert.h"

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_exists::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_exists::getBoolVal(Row& row, FunctionParm& fp, bool& isNull,
                                  CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  int jsErr = 0;
  json_engine_t jsEg;
  initJSEngine(jsEg, getCharset(fp[0]), js);

  if (!path.parsed && parseJSPath(path, row, fp[1]))
    goto error;

  if (locateJSPath(jsEg, path, &jsErr))
  {
    if (jsErr)
      goto error;
    return false;
  }

  return true;

error:
  isNull = true;
  return false;
}
}  // namespace funcexp
