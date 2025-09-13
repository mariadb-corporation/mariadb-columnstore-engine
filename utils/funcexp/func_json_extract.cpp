// Glaze first
#include <glaze/glaze.hpp>

#include <cctype>
#include "functor_json.h"
#include "glaze_path.h"
#include "rowgroup.h"
#include "treenode.h"
#include "mcs_decimal.h"

namespace funcexp
{
namespace
{
static json_value_types map_type(const glz::json_t& v)
{
  if (v.is_object())
    return JSON_VALUE_OBJECT;
  if (v.is_array())
    return JSON_VALUE_ARRAY;
  if (v.is_string())
    return JSON_VALUE_STRING;
  if (v.is_number())
    return JSON_VALUE_NUMBER;
  if (v.is_boolean())
    return v.get_boolean() ? JSON_VALUE_TRUE : JSON_VALUE_FALSE;
  return JSON_VALUE_NULL;
}
}  // namespace

int Func_json_extract::doExtract(rowgroup::Row& row, FunctionParm& fp, json_value_types* type,
                                 std::string& retJS, bool compareWhole)
{
  bool isNull = false;
  const auto js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return 1;

  glz::json_t doc;
  if (auto e = glz::read_json(doc, js.unsafeStringRef()))
    return 1;

  const size_t argSize = fp.size();
  if (argSize <= 1)
    return 1;

  // Multiple paths -> array of results (null for not found), now with wildcards and recursive descent
  std::vector<glz::json_t> results;
  results.reserve(argSize - 1);

  size_t found_count = 0;
  for (size_t i = 1; i < argSize; ++i)
  {
    bool pNull = false;
    const auto pstr_ns = fp[i]->data()->getStrVal(row, pNull);
    if (pNull)
      continue;  // skip this path entirely

    std::vector<const glz::json_t*> matches;
    if (!glaze_path::find_matches(doc, pstr_ns.unsafeStringRef(), matches))
      continue;  // skip invalid path

    if (matches.empty())
      continue;  // skip paths with no matches

    if (compareWhole)
    {
      // For compareWhole:
      // - If exactly one match for this path, push the value directly
      // - If multiple matches (due to wildcards), push an array of matches
      if (matches.size() == 1)
      {
        results.push_back(*matches.front());
        ++found_count;
      }
      else
      {
        glz::json_t arr;
        // Ensure variant holds an array before accessing it
        arr = std::vector<glz::json_t>{};
        auto& a = arr.get_array();
        a.reserve(matches.size());
        for (auto* m : matches)
          a.push_back(*m);
        results.push_back(std::move(arr));
        ++found_count;
      }
    }
    else
    {
      // For scalar conversions: pick the first match
      results.push_back(*matches.front());
      ++found_count;
    }
  }

  if (found_count == 0)
    return 1;

  // If only one path and compareWhole true and results[0] not an array-of-matches, return value directly
  glz::json_t out_json;
  if (results.size() == 1 && compareWhole)
  {
    out_json = results[0];
    *type = map_type(out_json);
  }
  else
  {
    // Ensure variant holds an array before assigning
    out_json = std::vector<glz::json_t>{};
    out_json.get_array() = std::move(results);
    *type = JSON_VALUE_ARRAY;
  }

  retJS.clear();
  if (auto w = writeJson(out_json, retJS))
    return 1;

  return 0;
}

execplan::CalpontSystemCatalog::ColType Func_json_extract::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_extract::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                         execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  std::string retJS;
  json_value_types valType;
  if (doExtract(row, fp, &valType, retJS, true) == 0)
    return retJS;

  isNull = true;
  return "";
}

int64_t Func_json_extract::getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& /*isNull*/,
                                     execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  std::string retJS;
  json_value_types valType;
  int64_t ret = 0;
  if (doExtract(row, fp, &valType, retJS, false) == 0)
  {
    if (valType == JSON_VALUE_TRUE)
      return 1;
    if (valType == JSON_VALUE_NUMBER || valType == JSON_VALUE_STRING)
    {
      char* end;
      int err;
      const CHARSET_INFO* cs = fp[0]->data()->resultType().getCharset();
      ret = cs->strntoll(retJS.data(), retJS.size(), 10, &end, &err);
    }
  }

  return ret;
}

double Func_json_extract::getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& /*isNull*/,
                                       execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  std::string retJS;
  json_value_types valType;
  double ret = 0.0;
  if (doExtract(row, fp, &valType, retJS, false) == 0)
  {
    if (valType == JSON_VALUE_TRUE)
      return 1.0;
    if (valType == JSON_VALUE_NUMBER || valType == JSON_VALUE_STRING)
    {
      char* end;
      int err;
      const CHARSET_INFO* cs = fp[0]->data()->resultType().getCharset();
      ret = cs->strntod(retJS.data(), retJS.size(), &end, &err);
    }
  }

  return ret;
}

datatypes::Decimal Func_json_extract::getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                                    execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  json_value_types valType;
  std::string retJS;

  if (doExtract(row, fp, &valType, retJS, false) == 0)
  {
    switch (valType)
    {
      case JSON_VALUE_STRING:
      case JSON_VALUE_NUMBER: return fp[0]->data()->getDecimalVal(row, isNull);
      case JSON_VALUE_TRUE: return datatypes::Decimal(1, 0, 1);
      case JSON_VALUE_OBJECT:
      case JSON_VALUE_ARRAY:
      case JSON_VALUE_FALSE:
      case JSON_VALUE_NULL:
      case JSON_VALUE_UNINITIALIZED: break;
    };
  }

  return datatypes::Decimal(0, 0, 1);
}
}  // namespace funcexp
