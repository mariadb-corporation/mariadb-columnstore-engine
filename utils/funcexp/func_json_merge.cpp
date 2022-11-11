#include "functor_json.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace
{
int doMerge(string& retJS, json_engine_t* jsEg1, json_engine_t* jsEg2)
{
  if (json_read_value(jsEg1) || json_read_value(jsEg2))
    return 1;

  if (jsEg1->value_type == JSON_VALUE_OBJECT && jsEg2->value_type == JSON_VALUE_OBJECT)
  {
    json_engine_t savJSEg1 = *jsEg1;
    json_engine_t savJSEg2 = *jsEg2;

    int firstKey = 1;
    json_string_t keyName;

    json_string_set_cs(&keyName, jsEg1->s.cs);

    retJS.append("{");
    while (json_scan_next(jsEg1) == 0 && jsEg1->state != JST_OBJ_END)
    {
      const uchar *keyStart, *keyEnd;
      /* Loop through the Json_1 keys and compare with the Json_2 keys. */
      DBUG_ASSERT(jsEg1->state == JST_KEY);
      keyStart = jsEg1->s.c_str;
      do
      {
        keyEnd = jsEg1->s.c_str;
      } while (json_read_keyname_chr(jsEg1) == 0);

      if (unlikely(jsEg1->s.error))
        return 1;

      if (firstKey)
        firstKey = 0;
      else
      {
        retJS.append(", ");
        *jsEg2 = savJSEg2;
      }

      retJS.append("\"");
      retJS.append((const char*)keyStart, (size_t)(keyEnd - keyStart));
      retJS.append("\":");

      while (json_scan_next(jsEg2) == 0 && jsEg2->state != JST_OBJ_END)
      {
        int ires;
        DBUG_ASSERT(jsEg2->state == JST_KEY);
        json_string_set_str(&keyName, keyStart, keyEnd);
        if (!json_key_matches(jsEg2, &keyName))
        {
          if (jsEg2->s.error || json_skip_key(jsEg2))
            return 2;
          continue;
        }

        /* Json_2 has same key as Json_1. Merge them. */
        if ((ires = doMerge(retJS, jsEg1, jsEg2)))
          return ires;
        goto merged_j1;
      }
      if (unlikely(jsEg2->s.error))
        return 2;

      keyStart = jsEg1->s.c_str;
      /* Just append the Json_1 key value. */
      if (json_skip_key(jsEg1))
        return 1;

      retJS.append((const char*)keyStart, jsEg1->s.c_str - keyStart);

    merged_j1:
      continue;
    }

    *jsEg2 = savJSEg2;
    /*
      Now loop through the Json_2 keys.
      Skip if there is same key in Json_1
    */
    while (json_scan_next(jsEg2) == 0 && jsEg2->state != JST_OBJ_END)
    {
      const uchar *keyStart, *keyEnd;
      DBUG_ASSERT(jsEg2->state == JST_KEY);
      keyStart = jsEg2->s.c_str;
      do
      {
        keyEnd = jsEg2->s.c_str;
      } while (json_read_keyname_chr(jsEg2) == 0);

      if (unlikely(jsEg2->s.error))
        return 1;

      *jsEg1 = savJSEg1;
      while (json_scan_next(jsEg1) == 0 && jsEg1->state != JST_OBJ_END)
      {
        DBUG_ASSERT(jsEg1->state == JST_KEY);
        json_string_set_str(&keyName, keyStart, keyEnd);
        if (!json_key_matches(jsEg1, &keyName))
        {
          if (unlikely(jsEg1->s.error || json_skip_key(jsEg1)))
            return 2;
          continue;
        }
        if (json_skip_key(jsEg2) || json_skip_level(jsEg1))
          return 1;
        goto continue_j2;
      }

      if (unlikely(jsEg1->s.error))
        return 2;

      if (firstKey)
        firstKey = 0;
      else
        retJS.append(", ");

      if (json_skip_key(jsEg2))
        return 1;

      retJS.append("\"");
      retJS.append((const char*)keyStart, jsEg2->s.c_str - keyStart);

    continue_j2:
      continue;
    }

    retJS.append("}");
  }
  else
  {
    const uchar *end1, *beg1, *end2, *beg2;
    int itemSize1 = 1, itemSize2 = 1;

    beg1 = jsEg1->value_begin;

    /* Merge as a single array. */
    if (jsEg1->value_type == JSON_VALUE_ARRAY)
    {
      if (json_skip_level_and_count(jsEg1, &itemSize1))
        return 1;

      end1 = jsEg1->s.c_str - jsEg1->sav_c_len;
    }
    else
    {
      retJS.append("[");

      if (jsEg1->value_type == JSON_VALUE_OBJECT)
      {
        if (json_skip_level(jsEg1))
          return 1;
        end1 = jsEg1->s.c_str;
      }
      else
        end1 = jsEg1->value_end;
    }

    retJS.append((const char*)beg1, end1 - beg1);

    if (json_value_scalar(jsEg2))
    {
      beg2 = jsEg2->value_begin;
      end2 = jsEg2->value_end;
    }
    else
    {
      if (jsEg2->value_type == JSON_VALUE_OBJECT)
      {
        beg2 = jsEg2->value_begin;
        if (json_skip_level(jsEg2))
          return 2;
      }
      else
      {
        beg2 = jsEg2->s.c_str;
        if (json_skip_level_and_count(jsEg2, &itemSize2))
          return 2;
      }
      end2 = jsEg2->s.c_str;
    }

    if (itemSize1 && itemSize2)
      retJS.append(", ");

    retJS.append((const char*)beg2, end2 - beg2);

    if (jsEg2->value_type != JSON_VALUE_ARRAY)
      retJS.append("]");
  }

  return 0;
}
}  // namespace

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_merge::operationType(FunctionParm& fp,
                                                             CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_merge::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  const CHARSET_INFO* js1CS = getCharset(fp[0]);

  json_engine_t jsEg1, jsEg2;

  string tmpJS{js};
  string retJS;

  for (size_t i = 1; i < fp.size(); i++)
  {
    const string_view js2 = fp[i]->data()->getStrVal(row, isNull);
    if (isNull)
      goto error;

    initJSEngine(jsEg1, js1CS, tmpJS);
    initJSEngine(jsEg2, getCharset(fp[i]), js2);

    if (doMerge(retJS, &jsEg1, &jsEg2))
      goto error;

    // tmpJS save the merge result for next loop
    tmpJS.swap(retJS);
    retJS.clear();
  }

  initJSEngine(jsEg1, js1CS, tmpJS);
  retJS.clear();
  if (doFormat(&jsEg1, retJS, Func_json_format::LOOSE))
    goto error;

  isNull = false;
  return retJS;

error:
  isNull = true;
  return "";
}
}  // namespace funcexp
