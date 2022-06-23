#include "functor_json.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;
using namespace rowgroup;

#include "dataconvert.h"

#include "jsonhelpers.h"
using namespace funcexp::helpers;
namespace
{
int checkOverlapsWithObj(json_engine_t* jsEg, json_engine_t* jsEg2, bool compareWhole);
bool checkOverlaps(json_engine_t* jsEg1, json_engine_t* jsEg2, bool compareWhole);

/*
  When the two values match or don't match we need to return true or false.
  But we can have some more elements in the array left or some more keys
  left in the object that we no longer want to compare. In this case,
  we want to skip the current item.
*/
void jsonSkipCurrLevel(json_engine_t* jsEg1, json_engine_t* jsEg2)
{
  json_skip_level(jsEg1);
  json_skip_level(jsEg2);
}
/* At least one of the two arguments is a scalar. */
bool checkOverlapsWithScalar(json_engine_t* jsEg1, json_engine_t* jsEg2)
{
  if (json_value_scalar(jsEg2))
  {
    if (jsEg1->value_type == jsEg2->value_type)
    {
      if (jsEg1->value_type == JSON_VALUE_NUMBER)
      {
        double dj, dv;
        char* end;
        int err;

        dj = jsEg1->s.cs->strntod((char*)jsEg1->value, jsEg1->value_len, &end, &err);
        dv = jsEg2->s.cs->strntod((char*)jsEg2->value, jsEg2->value_len, &end, &err);

        return (fabs(dj - dv) < 1e-12);
      }
      else if (jsEg1->value_type == JSON_VALUE_STRING)
      {
        return jsEg2->value_len == jsEg1->value_len &&
               memcmp(jsEg2->value, jsEg1->value, jsEg2->value_len) == 0;
      }
    }
    return jsEg2->value_type == jsEg1->value_type;
  }
  else if (jsEg2->value_type == JSON_VALUE_ARRAY)
  {
    while (json_scan_next(jsEg2) == 0 && jsEg2->state == JST_VALUE)
    {
      if (json_read_value(jsEg2))
        return false;
      if (jsEg1->value_type == jsEg2->value_type)
      {
        int res1 = checkOverlapsWithScalar(jsEg1, jsEg2);
        if (res1)
          return true;
      }
      if (!json_value_scalar(jsEg2))
        json_skip_level(jsEg2);
    }
  }
  return false;
}

/*
  Compare when one is object and other is array. This means we are looking
  for the object in the array. Hence, when value type of an element of the
  array is object, then compare the two objects entirely. If they are
  equal return true else return false.
*/
bool jsonCmpWithArrAndObj(json_engine_t* jsEg1, json_engine_t* jsEg2)
{
  st_json_engine_t locjsEg2 = *jsEg2;
  while (json_scan_next(jsEg1) == 0 && jsEg1->state == JST_VALUE)
  {
    if (json_read_value(jsEg1))
      return false;
    if (jsEg1->value_type == JSON_VALUE_OBJECT)
    {
      int res1 = checkOverlapsWithObj(jsEg1, jsEg2, true);
      if (res1)
        return true;
      *jsEg2 = locjsEg2;
    }
    if (!json_value_scalar(jsEg1))
      json_skip_level(jsEg1);
  }
  return false;
}

bool jsonCmpArrInOrder(json_engine_t* jsEg1, json_engine_t* jsEg2)
{
  bool res = false;
  while (json_scan_next(jsEg1) == 0 && json_scan_next(jsEg2) == 0 && jsEg1->state == JST_VALUE &&
         jsEg2->state == JST_VALUE)
  {
    if (json_read_value(jsEg1) || json_read_value(jsEg2))
      return false;
    if (jsEg1->value_type != jsEg2->value_type)
    {
      jsonSkipCurrLevel(jsEg1, jsEg2);
      return false;
    }
    res = checkOverlaps(jsEg1, jsEg2, true);
    if (!res)
    {
      jsonSkipCurrLevel(jsEg1, jsEg2);
      return false;
    }
  }
  res = (jsEg2->state == JST_ARRAY_END || jsEg2->state == JST_OBJ_END ? true : false);
  jsonSkipCurrLevel(jsEg1, jsEg2);
  return res;
}

int checkOverlapsWithArr(json_engine_t* jsEg1, json_engine_t* jsEg2, bool compareWhole)
{
  if (jsEg2->value_type == JSON_VALUE_ARRAY)
  {
    if (compareWhole)
      return jsonCmpArrInOrder(jsEg1, jsEg2);

    json_engine_t locjsEg2ue = *jsEg2, currJSEg = *jsEg1;

    while (json_scan_next(jsEg1) == 0 && jsEg1->state == JST_VALUE)
    {
      if (json_read_value(jsEg1))
        return false;
      currJSEg = *jsEg1;
      while (json_scan_next(jsEg2) == 0 && jsEg2->state == JST_VALUE)
      {
        if (json_read_value(jsEg2))
          return false;
        if (jsEg1->value_type == jsEg2->value_type)
        {
          int res1 = checkOverlaps(jsEg1, jsEg2, true);
          if (res1)
            return true;
        }
        else
        {
          if (!json_value_scalar(jsEg2))
            json_skip_level(jsEg2);
        }
        *jsEg1 = currJSEg;
      }
      *jsEg2 = locjsEg2ue;
      if (!json_value_scalar(jsEg1))
        json_skip_level(jsEg1);
    }
    return false;
  }
  else if (jsEg2->value_type == JSON_VALUE_OBJECT)
  {
    if (compareWhole)
    {
      jsonSkipCurrLevel(jsEg1, jsEg2);
      return false;
    }
    return jsonCmpWithArrAndObj(jsEg1, jsEg2);
  }
  else
    return checkOverlapsWithScalar(jsEg2, jsEg1);
}

int checkOverlapsWithObj(json_engine_t* jsEg1, json_engine_t* jsEg2, bool compareWhole)
{
  if (jsEg2->value_type == JSON_VALUE_OBJECT)
  {
    /* Find at least one common key-value pair */
    json_string_t keyName;
    bool foundKey = false, foundVal = false;
    json_engine_t locJSEg = *jsEg1;
    const uchar *keyStart, *keyEnd;

    json_string_set_cs(&keyName, jsEg2->s.cs);

    while (json_scan_next(jsEg2) == 0 && jsEg2->state == JST_KEY)
    {
      keyStart = jsEg2->s.c_str;
      do
      {
        keyEnd = jsEg2->s.c_str;
      } while (json_read_keyname_chr(jsEg2) == 0);

      if (unlikely(jsEg2->s.error))
        return false;

      json_string_set_str(&keyName, keyStart, keyEnd);
      foundKey = findKeyInObject(jsEg1, &keyName);
      foundVal = 0;

      if (foundKey)
      {
        if (json_read_value(jsEg1) || json_read_value(jsEg2))
          return false;

        /*
          The value of key-value pair can be an be anything. If it is an object
          then we need to compare the whole value and if it is an array then
          we need to compare the elements in that order. So set compareWhole
          to true.
        */
        if (jsEg1->value_type == jsEg2->value_type)
          foundVal = checkOverlaps(jsEg1, jsEg2, true);
        if (foundVal)
        {
          if (!compareWhole)
            return true;
          *jsEg1 = locJSEg;
        }
        else
        {
          if (compareWhole)
          {
            jsonSkipCurrLevel(jsEg1, jsEg2);
            return false;
          }
          *jsEg1 = locJSEg;
        }
      }
      else
      {
        if (compareWhole)
        {
          jsonSkipCurrLevel(jsEg1, jsEg2);
          return false;
        }
        json_skip_key(jsEg2);
        *jsEg1 = locJSEg;
      }
    }
    jsonSkipCurrLevel(jsEg1, jsEg2);
    return compareWhole ? true : false;
  }
  else if (jsEg2->value_type == JSON_VALUE_ARRAY)
  {
    if (compareWhole)
    {
      jsonSkipCurrLevel(jsEg1, jsEg2);
      return false;
    }
    return jsonCmpWithArrAndObj(jsEg2, jsEg1);
  }
  return false;
}

bool checkOverlaps(json_engine_t* jsEg1, json_engine_t* jsEg2, bool compareWhole)
{
  switch (jsEg1->value_type)
  {
    case JSON_VALUE_OBJECT: return checkOverlapsWithObj(jsEg1, jsEg2, compareWhole);
    case JSON_VALUE_ARRAY: return checkOverlapsWithArr(jsEg1, jsEg2, compareWhole);
    default: return checkOverlapsWithScalar(jsEg1, jsEg2);
  }
  return false;
}
}  // namespace

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_overlaps::operationType(FunctionParm& fp,
                                                                CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 */
bool Func_json_overlaps::getBoolVal(Row& row, FunctionParm& fp, bool& isNull,
                                    CalpontSystemCatalog::ColType& type)
{
  bool isNullJS1 = false, isNullJS2 = false;
  const string_view js1 = fp[0]->data()->getStrVal(row, isNullJS1);
  const string_view js2 = fp[1]->data()->getStrVal(row, isNullJS2);
  if (isNullJS1 || isNullJS2)
    return false;

  json_engine_t jsEg1, jsEg2;
  initJSEngine(jsEg1, getCharset(fp[0]), js1);
  initJSEngine(jsEg2, getCharset(fp[1]), js2);

  if (json_read_value(&jsEg1) || json_read_value(&jsEg2))
    return false;

  bool result = checkOverlaps(&jsEg1, &jsEg2, false);
  if (unlikely(jsEg1.s.error || jsEg2.s.error))
    return false;

  return result;
}
}  // namespace funcexp
