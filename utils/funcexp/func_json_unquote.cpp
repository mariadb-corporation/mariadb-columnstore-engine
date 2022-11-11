#include "functor_json.h"
#include "functioncolumn.h"
#include "jsonhelpers.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "jsonhelpers.h"
using namespace funcexp::helpers;
namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_unquote::operationType(FunctionParm& fp,
                                                               CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_unquote::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                         execplan::CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  json_engine_t jsEg;
  int strLen;

  const CHARSET_INFO* cs = type.getCharset();
  initJSEngine(jsEg, cs, js);

  json_read_value(&jsEg);

  if (unlikely(jsEg.s.error) || jsEg.value_type != JSON_VALUE_STRING)
    return js.data();

  char* buf = (char*)alloca(jsEg.value_len);
  if ((strLen = json_unescape(cs, jsEg.value, jsEg.value + jsEg.value_len, &my_charset_utf8mb3_general_ci,
                              (uchar*)buf, (uchar*)(buf + jsEg.value_len))) >= 0)
  {
    buf[strLen] = '\0';
    string ret = buf;
    return strLen == 0 ? "" : ret;
  }

  return js.data();
}
}  // namespace funcexp
