#include "jsonhelpers.h"
using namespace std;

namespace funcexp
{
namespace helpers
{
int setupJSPath(json_path_t* path, CHARSET_INFO* cs, const string_view& str, bool wildcards = true)
{
  int err = json_path_setup(path, cs, (const uchar*)str.data(), (const uchar*)str.data() + str.size());
  if (wildcards)
    return err;

  if (!err)
  {
#ifdef MYSQL_GE_1009
    bool support = (path->types_used & (JSON_PATH_WILD | JSON_PATH_DOUBLE_WILD | JSON_PATH_ARRAY_RANGE)) == 0;
#else
    bool support = (path->types_used & (JSON_PATH_WILD | JSON_PATH_DOUBLE_WILD)) == 0;
#endif
    if (support)
      return 0;
    path->s.error = NO_WILDCARD_ALLOWED;
  }
  return 1;
}

bool appendEscapedJS(string& ret, const CHARSET_INFO* retCS, const string_view& js, const CHARSET_INFO* jsCS)
{
  const int jsLen = js.size();
  const char* rawJS = js.data();
  int strLen = jsLen * 12 * jsCS->mbmaxlen / jsCS->mbminlen;
  char* buf = (char*)alloca(strLen);
  if ((strLen = json_escape(retCS, (const uchar*)rawJS, (const uchar*)rawJS + jsLen, jsCS, (uchar*)buf,
                            (uchar*)buf + strLen)) > 0)
  {
    buf[strLen] = '\0';
    ret.append(buf, strLen);
    return false;
  }

  return true;
}

bool appendJSKeyName(string& ret, const CHARSET_INFO* retCS, rowgroup::Row& row, execplan::SPTP& parm)
{
  bool nullVal = false;
  const string_view js = parm->data()->getStrVal(row, nullVal);
  if (nullVal)
  {
    ret.append("\"\": ");
    return false;
  }

  ret.append("\"");
  if (appendEscapedJS(ret, retCS, js, parm->data()->resultType().getCharset()))
    return true;
  ret.append("\": ");
  return false;
}

bool appendJSValue(string& ret, const CHARSET_INFO* retCS, rowgroup::Row& row, execplan::SPTP& parm)
{
  bool nullVal = false;
  const string_view js = parm->data()->getStrVal(row, nullVal);
  if (nullVal)
  {
    ret.append("null");
    return false;
  }

  datatypes::SystemCatalog::ColDataType dataType = parm->data()->resultType().colDataType;
  if (dataType == datatypes::SystemCatalog::BIGINT && (js == "true" || js == "false"))
  {
    ret.append(js);
    return false;
  }

  const CHARSET_INFO* jsCS = parm->data()->resultType().getCharset();
  if (isCharType(dataType))
  {
    ret.append("\"");
    if (appendEscapedJS(ret, retCS, js, jsCS))
      return true;
    ret.append("\"");
    return false;
  }

  return appendEscapedJS(ret, retCS, js, jsCS);
}

int appendTab(string& js, const int depth, const int tabSize)
{
  try
  {
    js.append("\n");
    for (int i = 0; i < depth; i++)
    {
      js.append(tab_arr, tabSize);
    }
  }
  catch (const std::exception& e)
  {
    return 1;
  }
  return 0;
}

int doFormat(json_engine_t* je, string& niceJS, Func_json_format::FORMATS mode, int tabSize)
{
  int depth = 0;
  static const char *comma = ", ", *colon = "\": ";
  uint commaLen, colonLen;
  int firstValue = 1;

  niceJS.reserve(je->s.str_end - je->s.c_str + 32);

  assert(mode != Func_json_format::DETAILED || (tabSize >= 0 && tabSize <= TAB_SIZE_LIMIT));

  if (mode == Func_json_format::LOOSE)
  {
    commaLen = 2;
    colonLen = 3;
  }
  else if (mode == Func_json_format::DETAILED)
  {
    commaLen = 1;
    colonLen = 3;
  }
  else
  {
    commaLen = 1;
    colonLen = 2;
  }

  do
  {
    switch (je->state)
    {
      case JST_KEY:
      {
        const uchar* key_start = je->s.c_str;
        const uchar* key_end;

        do
        {
          key_end = je->s.c_str;
        } while (json_read_keyname_chr(je) == 0);

        if (unlikely(je->s.error))
          goto error;

        if (!firstValue)
          niceJS.append(comma, commaLen);

        if (mode == Func_json_format::DETAILED && appendTab(niceJS, depth, tabSize))
          goto error;

        niceJS.append("\"");
        niceJS.append((const char*)key_start, (int)(key_end - key_start));
        niceJS.append(colon, colonLen);
      }
        /* now we have key value to handle, so no 'break'. */
        DBUG_ASSERT(je->state == JST_VALUE);
        goto handle_value;

      case JST_VALUE:
        if (!firstValue)
          niceJS.append(comma, commaLen);

        if (mode == Func_json_format::DETAILED && depth > 0 && appendTab(niceJS, depth, tabSize))
          goto error;

      handle_value:
        if (json_read_value(je))
          goto error;
        if (json_value_scalar(je))
        {
          niceJS.append((const char*)je->value_begin, (int)(je->value_end - je->value_begin));

          firstValue = 0;
        }
        else
        {
          if (mode == Func_json_format::DETAILED && depth > 0 && appendTab(niceJS, depth, tabSize))
            goto error;
          niceJS.append((je->value_type == JSON_VALUE_OBJECT) ? "{" : "[");
          firstValue = 1;
          depth++;
        }

        break;

      case JST_OBJ_END:
      case JST_ARRAY_END:
        depth--;
        if (mode == Func_json_format::DETAILED && appendTab(niceJS, depth, tabSize))
          goto error;
        niceJS.append((je->state == JST_OBJ_END) ? "}" : "]");
        firstValue = 0;
        break;

      default: break;
    };
  } while (json_scan_next(je) == 0);

  return je->s.error || *je->killed_ptr;

error:
  return 1;
}

bool findKeyInObject(json_engine_t* jsEg, json_string_t* key)
{
  const uchar* str = key->c_str;

  while (json_scan_next(jsEg) == 0 && jsEg->state != JST_OBJ_END)
  {
    DBUG_ASSERT(jsEg->state == JST_KEY);
    if (json_key_matches(jsEg, key))
      return true;
    if (json_skip_key(jsEg))
      return false;
    key->c_str = str;
  }

  return false;
}

int cmpPartJSPath(const json_path_step_t* a, const json_path_step_t* aEnd, const json_path_step_t* b,
                  const json_path_step_t* bEnd, enum json_value_types vt, const int* arraySize)
{
  int ret, ret2;
  const json_path_step_t* tmpB = b;

  while (a <= aEnd)
  {
    if (b > bEnd)
    {
      while (vt != JSON_VALUE_ARRAY && (a->type & JSON_PATH_ARRAY_WILD) == JSON_PATH_ARRAY && a->n_item == 0)
      {
        if (++a > aEnd)
          return 0;
      }
      return -2;
    }

    DBUG_ASSERT((b->type & (JSON_PATH_WILD | JSON_PATH_DOUBLE_WILD)) == 0);

    if (a->type & JSON_PATH_ARRAY)
    {
      if (b->type & JSON_PATH_ARRAY)
      {
#ifdef MYSQL_GE_1009
        int ret = 0, corrected_n_item_a = 0;
        if (arraySize)
          corrected_n_item_a = a->n_item < 0 ? arraySize[b - tmpB] + a->n_item : a->n_item;
        if (a->type & JSON_PATH_ARRAY_RANGE)
        {
          int corrected_n_item_end_a = 0;
          if (arraySize)
            corrected_n_item_end_a = a->n_item_end < 0 ? arraySize[b - tmpB] + a->n_item_end : a->n_item_end;
          ret = b->n_item >= corrected_n_item_a && b->n_item <= corrected_n_item_end_a;
        }
        else
          ret = corrected_n_item_a == b->n_item;

        if ((a->type & JSON_PATH_WILD) || ret)
          goto step_fits;
        goto step_failed;
#else
        if ((a->type & JSON_PATH_WILD) || a->n_item == b->n_item)
          goto step_fits;
        goto step_failed;
#endif
      }
      if ((a->type & JSON_PATH_WILD) == 0 && a->n_item == 0)
        goto step_fits_autowrap;
      goto step_failed;
    }
    else /* JSON_PATH_KEY */
    {
      if (!(b->type & JSON_PATH_KEY))
        goto step_failed;

      if (!(a->type & JSON_PATH_WILD) &&
          (a->key_end - a->key != b->key_end - b->key || memcmp(a->key, b->key, a->key_end - a->key) != 0))
        goto step_failed;

      goto step_fits;
    }
  step_failed:
    if (!(a->type & JSON_PATH_DOUBLE_WILD))
      return -1;
    b++;
    continue;

  step_fits:
    b++;
    if (!(a->type & JSON_PATH_DOUBLE_WILD))
    {
      a++;
      continue;
    }

    /* Double wild handling needs recursions. */
    ret = cmpPartJSPath(a + 1, aEnd, b, bEnd, vt, arraySize ? arraySize + (b - tmpB) : NULL);
    if (ret == 0)
      return 0;

    ret2 = cmpPartJSPath(a, aEnd, b, bEnd, vt, arraySize ? arraySize + (b - tmpB) : NULL);

    return (ret2 >= 0) ? ret2 : ret;

  step_fits_autowrap:
    if (!(a->type & JSON_PATH_DOUBLE_WILD))
    {
      a++;
      continue;
    }

    /* Double wild handling needs recursions. */
    ret = cmpPartJSPath(a + 1, aEnd, b + 1, bEnd, vt, arraySize ? arraySize + (b - tmpB) : NULL);
    if (ret == 0)
      return 0;

    ret2 = cmpPartJSPath(a, aEnd, b + 1, bEnd, vt, arraySize ? arraySize + (b - tmpB) : NULL);

    return (ret2 >= 0) ? ret2 : ret;
  }

  return b <= bEnd;
}

int cmpJSPath(const json_path_t* a, const json_path_t* b, enum json_value_types vt, const int* arraySize)
{
  return cmpPartJSPath(a->steps + 1, a->last_step, b->steps + 1, b->last_step, vt, arraySize);
}

int parseJSPath(JSONPath& path, rowgroup::Row& row, execplan::SPTP& parm, bool wildcards)
{
  // check if path column is const
  if (!path.constant)
    markConstFlag(path, parm);

  bool isNull = false;
  const string_view jsp = parm->data()->getStrVal(row, isNull);

  if (isNull || setupJSPath(&path.p, getCharset(parm), jsp, wildcards))
    return 1;

  path.parsed = path.constant;

  return 0;
}

bool matchJSPath(const vector<funcexp::JSONPath>& paths, const json_path_t* p, json_value_types valType,
                 const int* arrayCounter, bool exact)
{
  for (size_t curr = 0; curr < paths.size(); curr++)
  {
#ifdef MYSQL_GE_1009
    int cmp = cmpJSPath(&paths[curr].p, p, valType, arrayCounter);
#else
    int cmp = cmpJSPath(&paths[curr].p, p, valType);
#endif
    bool ret = exact ? cmp >= 0 : cmp == 0;
    if (ret)
      return true;
  }
  return false;
}
}  // namespace helpers
}  // namespace funcexp
