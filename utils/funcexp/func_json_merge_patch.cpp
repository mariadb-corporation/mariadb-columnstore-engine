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
int copyValuePatch(string& retJS, json_engine_t* jsEg)
{
  int firstKey = 1;

  if (jsEg->value_type != JSON_VALUE_OBJECT)
  {
    const uchar *beg, *end;

    beg = jsEg->value_begin;

    if (!json_value_scalar(jsEg))
    {
      if (json_skip_level(jsEg))
        return 1;
      end = jsEg->s.c_str;
    }
    else
      end = jsEg->value_end;

    retJS.append((const char*)beg, end - beg);

    return 0;
  }
  /* JSON_VALUE_OBJECT */

  retJS.append("{");

  while (json_scan_next(jsEg) == 0 && jsEg->state != JST_OBJ_END)
  {
    const uchar* keyStart;
    /* Loop through the Json_1 keys and compare with the Json_2 keys. */
    DBUG_ASSERT(jsEg->state == JST_KEY);
    keyStart = jsEg->s.c_str;

    if (json_read_value(jsEg))
      return 1;

    if (jsEg->value_type == JSON_VALUE_NULL)
      continue;

    if (!firstKey)
      retJS.append(", ");
    else
      firstKey = 0;

    retJS.append("\"");
    retJS.append((const char*)keyStart, jsEg->value_begin - keyStart);
    if (copyValuePatch(retJS, jsEg))
      return 1;
  }

  retJS.append("}");

  return 0;
}

int doMergePatch(string& retJS, json_engine_t* jsEg1, json_engine_t* jsEg2, bool& isEmpty)
{
  if (json_read_value(jsEg1) || json_read_value(jsEg2))
    return 1;

  if (jsEg1->value_type == JSON_VALUE_OBJECT && jsEg2->value_type == JSON_VALUE_OBJECT)
  {
    json_engine_t savJSEg1 = *jsEg1;
    json_engine_t savJSEg2 = *jsEg2;

    int firstKey = 1;
    json_string_t keyName;
    size_t savLen;
    bool mrgEmpty;

    isEmpty = false;
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

      if (jsEg1->s.error)
        return 1;

      savLen = retJS.size();

      if (!firstKey)
      {
        retJS.append(", ");
        *jsEg2 = savJSEg2;
      }

      retJS.append("\"");
      retJS.append((const char*)keyStart, keyEnd - keyStart);
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
        if ((ires = doMergePatch(retJS, jsEg1, jsEg2, mrgEmpty)))
          return ires;

        if (mrgEmpty)
          retJS = retJS.substr(0, savLen);
        else
          firstKey = 0;

        goto merged_j1;
      }

      if (jsEg2->s.error)
        return 2;

      keyStart = jsEg1->s.c_str;
      /* Just append the Json_1 key value. */
      if (json_skip_key(jsEg1))
        return 1;
      retJS.append((const char*)keyStart, jsEg1->s.c_str - keyStart);
      firstKey = 0;

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

      if (jsEg2->s.error)
        return 1;

      *jsEg1 = savJSEg1;
      while (json_scan_next(jsEg1) == 0 && jsEg1->state != JST_OBJ_END)
      {
        DBUG_ASSERT(jsEg1->state == JST_KEY);
        json_string_set_str(&keyName, keyStart, keyEnd);
        if (!json_key_matches(jsEg1, &keyName))
        {
          if (jsEg1->s.error || json_skip_key(jsEg1))
            return 2;
          continue;
        }
        if (json_skip_key(jsEg2) || json_skip_level(jsEg1))
          return 1;
        goto continue_j2;
      }

      if (jsEg1->s.error)
        return 2;

      savLen = retJS.size();

      if (!firstKey)
        retJS.append(", ");

      retJS.append("\"");
      retJS.append((const char*)keyStart, keyEnd - keyStart);
      retJS.append("\":");

      if (json_read_value(jsEg2))
        return 1;

      if (jsEg2->value_type == JSON_VALUE_NULL)
        retJS = retJS.substr(0, savLen);
      else
      {
        if (copyValuePatch(retJS, jsEg2))
          return 1;
        firstKey = 0;
      }

    continue_j2:
      continue;
    }

    retJS.append("}");
  }
  else
  {
    if (!json_value_scalar(jsEg1) && json_skip_level(jsEg1))
      return 1;

    isEmpty = (jsEg2->value_type == JSON_VALUE_NULL);
    if (!isEmpty && copyValuePatch(retJS, jsEg2))
      return 1;
  }

  return 0;
}
}  // namespace

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_merge_patch::operationType(FunctionParm& fp,
                                                                   CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_merge_patch::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                        execplan::CalpontSystemCatalog::ColType& type)
{
  // JSON_MERGE_PATCH return NULL if any argument is NULL
  bool isEmpty = false, hasNullArg = false;
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  hasNullArg = isNull;
  if (isNull)
    isNull = false;

  json_engine_t jsEg1, jsEg2;
  jsEg1.s.error = jsEg2.s.error = 0;

  string tmpJS{js};
  string retJS;
  for (size_t i = 1; i < fp.size(); i++)
  {
    const string_view js2 = fp[i]->data()->getStrVal(row, isNull);
    if (isNull)
    {
      hasNullArg = true;
      isNull = false;
      goto next;
    }

    initJSEngine(jsEg2, getCharset(fp[i]), js2);

    if (hasNullArg)
    {
      if (json_read_value(&jsEg2))
        goto error;
      if (jsEg2.value_type == JSON_VALUE_OBJECT)
        goto next;

      hasNullArg = false;
      retJS.append(js2.data());
      goto next;
    }

    initJSEngine(jsEg1, getCharset(fp[0]), tmpJS);
    if (doMergePatch(retJS, &jsEg1, &jsEg2, isEmpty))
      goto error;

    if (isEmpty)
      retJS.append("null");

  next:
    // tmpJS save the merge result for next loop
    tmpJS.swap(retJS);
    retJS.clear();
  }

  if (hasNullArg)
    goto error;

  initJSEngine(jsEg1, getCharset(fp[0]), tmpJS);
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
