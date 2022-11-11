#include "functor_json.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
bool JSONEgWrapper::checkAndGetScalar(string& ret, int* error)
{
  CHARSET_INFO* cs;
  const uchar* js;
  uint jsLen;

  if (!json_value_scalar(this))
  {
    /* We only look for scalar values! */
    if (json_skip_level(this) || json_scan_next(this))
      *error = 1;
    return true;
  }

  if (value_type == JSON_VALUE_TRUE || value_type == JSON_VALUE_FALSE)
  {
    cs = &my_charset_utf8mb4_bin;
    js = (const uchar*)((value_type == JSON_VALUE_TRUE) ? "1" : "0");
    jsLen = 1;
  }
  else
  {
    cs = s.cs;
    js = value;
    jsLen = value_len;
  }

  int strLen = jsLen * cs->mbmaxlen;

  char* buf = (char*)alloca(jsLen + strLen);
  if ((strLen = json_unescape(cs, js, js + jsLen, cs, (uchar*)buf, (uchar*)buf + jsLen + strLen)) > 0)
  {
    buf[strLen] = '\0';
    ret.append(buf);
    return 0;
  }

  return strLen;
}

/*
  Returns NULL, not an error if the found value
  is not a scalar.
*/
bool JSONPathWrapper::extract(std::string& ret, rowgroup::Row& row, execplan::SPTP& funcParamJS,
                              execplan::SPTP& funcParamPath)
{
  bool isNullJS = false, isNullPath = false;

  const string& js = funcParamJS->data()->getStrVal(row, isNullJS);
  const string_view jsp = funcParamPath->data()->getStrVal(row, isNullPath);
  if (isNullJS || isNullPath)
    return true;

  int error = 0;

  if (!parsed)
  {
    if (!constant)
    {
      ConstantColumn* constCol = dynamic_cast<ConstantColumn*>(funcParamPath->data());
      constant = (constCol != nullptr);
    }

    if (isNullPath || json_path_setup(&p, getCharset(funcParamPath), (const uchar*)jsp.data(),
                                      (const uchar*)jsp.data() + jsp.size()))
      return true;

    parsed = constant;
  }

  JSONEgWrapper je(js, getCharset(funcParamJS));

  currStep = p.steps;

  do
  {
    if (error)
      return true;

    IntType arrayCounters[JSON_DEPTH_LIMIT];
    if (json_find_path(&je, &p, &currStep, arrayCounters))
      return true;

    if (json_read_value(&je))
      return true;

  } while (unlikely(checkAndGetValue(&je, ret, &error)));

  return false;
}

CalpontSystemCatalog::ColType Func_json_value::operationType(FunctionParm& fp,
                                                             CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_value::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& type)
{
  string ret;
  isNull = JSONPathWrapper::extract(ret, row, fp[0], fp[1]);
  return isNull ? "" : ret;
}
}  // namespace funcexp
