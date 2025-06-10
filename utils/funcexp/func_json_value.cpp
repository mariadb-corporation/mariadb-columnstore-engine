#include "functor_json.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
#include "json_lib.h"
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

#if MYSQL_VERSION_ID >= 120100
  MEM_ROOT_DYNAMIC_ARRAY array;
  IntType arrayCounters[JSON_DEPTH_LIMIT];
  int je_stack[JSON_DEPTH_LIMIT];
  json_path_step_t p_steps[JSON_DEPTH_LIMIT];
#endif

  const utils::NullString& js = funcParamJS->data()->getStrVal(row, isNullJS);
  const utils::NullString& sjsp = funcParamPath->data()->getStrVal(row, isNullPath);
  if (isNullJS || isNullPath)
    return true;

  int error = 0;

#if MYSQL_VERSION_ID >= 120100
  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &p.steps, sizeof(json_path_step_t), &p_steps,
                              JSON_DEPTH_LIMIT, 0, MYF(0));
#endif

  if (json_path_setup(&p, getCharset(funcParamPath), (const uchar*)sjsp.str(),
                                                     (const uchar*)sjsp.end()))
    return true;

  JSONEgWrapper je(getCharset(funcParamJS), reinterpret_cast<const uchar*>(js.str()), reinterpret_cast<const uchar*>(js.end()));

#if MYSQL_VERSION_ID >= 120100
  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &je.stack, sizeof(int), &je_stack,
                              JSON_DEPTH_LIMIT, 0, MYF(0));

  currStep = (json_path_step_t*)p.steps.buffer;

  
  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &array, sizeof(int), &arrayCounters,
                              JSON_DEPTH_LIMIT, 0, MYF(0));
#else
  currStep = p.steps;
#endif

  do
  {
    if (error)
      return true;

#if MYSQL_VERSION_ID >= 120100
    if (json_find_path(&je, &p, &currStep, &array))
#else
    IntType arrayCounters[JSON_DEPTH_LIMIT];
    if (json_find_path(&je, &p, &currStep, arrayCounters))
#endif
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

class JSONPathWrapperValue : public JSONPathWrapper
{
 public:
  JSONPathWrapperValue()
  {
  }
  virtual ~JSONPathWrapperValue()
  {
  }

  bool checkAndGetValue(JSONEgWrapper* je, string& res, int* error) override
  {
    return je->checkAndGetScalar(res, error);
  }

};

string Func_json_value::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& type)
{
  string ret;
  JSONPathWrapperValue pw;
  isNull = pw.extract(ret, row, fp[0], fp[1]);
  return isNull ? "" : ret;
}
}  // namespace funcexp
