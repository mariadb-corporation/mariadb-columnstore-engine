#include <string_view>
#include <memory>
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
CalpontSystemCatalog::ColType Func_json_equals::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_equals::getBoolVal(Row& row, FunctionParm& fp, bool& isNull,
                                  CalpontSystemCatalog::ColType& type)
{
  // auto release the DYNAMIC_STRING
  using DynamicString = unique_ptr<DYNAMIC_STRING, decltype(&dynstr_free)>;

  DynamicString str1{new DYNAMIC_STRING(), dynstr_free};
  if (init_dynamic_string(str1.get(), NULL, 0, 0))
  {
    isNull = true;
    return true;
  }

  DynamicString str2{new DYNAMIC_STRING(), dynstr_free};
  if (init_dynamic_string(str2.get(), NULL, 0, 0))
  {
    isNull = true;
    return true;
  }

  const string_view js1 = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  const string_view js2 = fp[1]->data()->getStrVal(row, isNull);
  if (isNull)
    return false;

  bool result = false;
  if (json_normalize(str1.get(), js1.data(), js1.size(), getCharset(fp[0])))
  {
    isNull = true;
    return result;
  }

  if (json_normalize(str2.get(), js2.data(), js2.size(), getCharset(fp[1])))
  {
    isNull = true;
    return result;
  }

  result = strcmp(str1->str, str2->str) ? false : true;
  return result;
}
}  // namespace funcexp
