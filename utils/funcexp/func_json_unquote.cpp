#include "functor_json.h"
#include "functioncolumn.h"
#include "json_lib.h"
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
                                                               CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_unquote::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                         execplan::CalpontSystemCatalog::ColType& type)
{
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  Func_json_no_multipath_state state;

  int strLen;

  const CHARSET_INFO* cs = type.getCharset();

  initJSEngine(state.jsEg, cs, js);

  json_read_value(&state.jsEg);

  if (unlikely(state.jsEg.s.error) || state.jsEg.value_type != JSON_VALUE_STRING)
    return js.safeString();

  char* buf = (char*)alloca(state.jsEg.value_len + 1);
  if ((strLen = json_unescape(cs, state.jsEg.value, state.jsEg.value + state.jsEg.value_len, &my_charset_utf8mb3_general_ci,
                              (uchar*)buf, (uchar*)(buf + state.jsEg.value_len))) >= 0)
  {
    buf[strLen] = '\0';
    std::string ret = buf;
    return strLen == 0 ? "" : ret;
  }

  return js.safeString("");
}
}  // namespace funcexp
