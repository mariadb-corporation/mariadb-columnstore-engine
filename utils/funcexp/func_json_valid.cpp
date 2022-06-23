#include <string_view>
using namespace std;

#include "functor_json.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;
using namespace rowgroup;

#include "dataconvert.h"

#include "jsonhelpers.h"
using namespace funcexp::helpers;
namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_valid::operationType(FunctionParm& fp,
                                                             CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_valid::getBoolVal(Row& row, FunctionParm& fp, bool& isNull,
                                 CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  return json_valid(js.data(), js.size(), getCharset(fp[0]));
}
}  // namespace funcexp
