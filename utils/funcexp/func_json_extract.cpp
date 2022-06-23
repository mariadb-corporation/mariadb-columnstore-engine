#include <type_traits>
#include "functor_json.h"
#include "functioncolumn.h"
#include "rowgroup.h"
#include "treenode.h"
using namespace execplan;
using namespace rowgroup;

#include "dataconvert.h"

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
int Func_json_extract::doExtract(Row& row, FunctionParm& fp, json_value_types* type, string& retJS,
                                 bool compareWhole = true)
{
  bool isNull = false;
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return 1;
  const char* rawJS = js.data();
  json_engine_t jsEg, savJSEg;
  json_path_t p;
  const uchar* value;
  bool notFirstVal = false;
  size_t valLen;
  bool mayMulVal;
  int wildcards;
  bool isMatch;
#ifdef MYSQL_GE_1009
  int arrayCounter[JSON_DEPTH_LIMIT];
  bool hasNegPath = false;
#endif
  const size_t argSize = fp.size();
  string tmp;

  initJSPaths(paths, fp, 1, 1);

  for (size_t i = 1; i < argSize; i++)
  {
    JSONPath& path = paths[i - 1];
    path.p.types_used = JSON_PATH_KEY_NULL;
    if (!path.parsed && parseJSPath(path, row, fp[i]))
      goto error;

#ifdef MYSQL_GE_1009
    hasNegPath |= path.p.types_used & JSON_PATH_NEGATIVE_INDEX;
#endif
  }

#ifdef MYSQL_GE_1009
  wildcards = (JSON_PATH_WILD | JSON_PATH_DOUBLE_WILD | JSON_PATH_ARRAY_RANGE);
#else
  wildcards = (JSON_PATH_WILD | JSON_PATH_DOUBLE_WILD);
#endif
  mayMulVal = argSize > 2 || (paths[0].p.types_used & wildcards);

  *type = mayMulVal ? JSON_VALUE_ARRAY : JSON_VALUE_NULL;

  if (compareWhole)
  {
    retJS.clear();
    if (mayMulVal)
      retJS.append("[");
  }

  json_get_path_start(&jsEg, getCharset(fp[0]), (const uchar*)rawJS, (const uchar*)rawJS + js.size(), &p);

  while (json_get_path_next(&jsEg, &p) == 0)
  {
#ifdef MYSQL_GE_1009
    if (hasNegPath && jsEg.value_type == JSON_VALUE_ARRAY &&
        json_skip_array_and_count(&jsEg, arrayCounter + (p.last_step - p.steps)))
      goto error;
#endif

#ifdef MYSQL_GE_1009
    isMatch = matchJSPath(paths, &p, jsEg.value_type, arrayCounter, false);
#else
    isMatch = matchJSPath(paths, &p, jsEg.value_type, nullptr, false);
#endif
    if (!isMatch)
      continue;

    value = jsEg.value_begin;
    if (*type == JSON_VALUE_NULL)
      *type = jsEg.value_type;

    /* we only care about the first found value */
    if (!compareWhole)
    {
      retJS = js;
      return 0;
    }

    if (json_value_scalar(&jsEg))
      valLen = jsEg.value_end - value;
    else
    {
      if (mayMulVal)
        savJSEg = jsEg;
      if (json_skip_level(&jsEg))
        goto error;
      valLen = jsEg.s.c_str - value;
      if (mayMulVal)
        jsEg = savJSEg;
    }

    if (notFirstVal)
      retJS.append(", ");
    retJS.append((const char*)value, valLen);

    notFirstVal = true;

    if (!mayMulVal)
    {
      /* Loop to the end of the JSON just to make sure it's valid. */
      while (json_get_path_next(&jsEg, &p) == 0)
      {
      }
      break;
    }
  }

  if (unlikely(jsEg.s.error))
    goto error;

  if (!notFirstVal)
    /* Nothing was found. */
    goto error;

  if (mayMulVal)
    retJS.append("]");

  initJSEngine(jsEg, getCharset(fp[0]), retJS);
  if (doFormat(&jsEg, tmp, Func_json_format::LOOSE))
    goto error;

  retJS.clear();
  retJS.swap(tmp);

  return 0;

error:
  return 1;
}

CalpontSystemCatalog::ColType Func_json_extract::operationType(FunctionParm& fp,
                                                               CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_extract::getStrVal(Row& row, FunctionParm& fp, bool& isNull,
                                    CalpontSystemCatalog::ColType& type)
{
  string retJS;
  json_value_types valType;
  if (doExtract(row, fp, &valType, retJS) == 0)
    return retJS;

  isNull = true;
  return "";
}

int64_t Func_json_extract::getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                     execplan::CalpontSystemCatalog::ColType& type)
{
  string retJS;
  json_value_types valType;
  int64_t ret = 0;
  if (doExtract(row, fp, &valType, retJS, false) == 0)
  {
    switch (valType)
    {
      case JSON_VALUE_NUMBER:
      case JSON_VALUE_STRING:
      {
        char* end;
        int err;
        ret = getCharset(fp[0])->strntoll(retJS.data(), retJS.size(), 10, &end, &err);
        break;
      }
      case JSON_VALUE_TRUE: ret = 1; break;
      default: break;
    };
  }

  return ret;
}

double Func_json_extract::getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                       execplan::CalpontSystemCatalog::ColType& type)
{
  string retJS;
  json_value_types valType;
  double ret = 0.0;
  if (doExtract(row, fp, &valType, retJS, false) == 0)
  {
    switch (valType)
    {
      case JSON_VALUE_NUMBER:
      case JSON_VALUE_STRING:
      {
        char* end;
        int err;
        ret = getCharset(fp[0])->strntod(retJS.data(), retJS.size(), &end, &err);
        break;
      }
      case JSON_VALUE_TRUE: ret = 1.0; break;
      default: break;
    };
  }

  return ret;
}

execplan::IDB_Decimal Func_json_extract::getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                                       execplan::CalpontSystemCatalog::ColType& type)
{
  json_value_types valType;
  string retJS;

  if (doExtract(row, fp, &valType, retJS, false) == 0)
  {
    switch (valType)
    {
      case JSON_VALUE_STRING:
      case JSON_VALUE_NUMBER: return fp[0]->data()->getDecimalVal(row, isNull);
      case JSON_VALUE_TRUE: return IDB_Decimal(1, 0, 1);
      case JSON_VALUE_OBJECT:
      case JSON_VALUE_ARRAY:
      case JSON_VALUE_FALSE:
      case JSON_VALUE_NULL:
      case JSON_VALUE_UNINITIALIZED: break;
    };
  }

  return IDB_Decimal(0, 0, 1);
}
}  // namespace funcexp
