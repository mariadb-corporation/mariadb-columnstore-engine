#include "functor_json.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "dataconvert.h"
using namespace dataconvert;

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace
{
bool checkKeyInList(const string& res, const uchar* key, const int keyLen)
{
  const uchar* curr = (const uchar*)res.c_str() + 2;             /* beginning '["' */
  const uchar* end = (const uchar*)res.c_str() + res.size() - 1; /* ending '"' */

  while (curr < end)
  {
    int i;
    for (i = 0; curr[i] != '"' && i < keyLen; i++)
    {
      if (curr[i] != key[i])
        break;
    }
    if (curr[i] == '"')
    {
      if (i == keyLen)
        return true;
    }
    else
    {
      while (curr[i] != '"')
        i++;
    }
    curr += i + 4; /* skip ', "' */
  }
  return false;
}
}  // namespace

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_keys::operationType(FunctionParm& fp,
                                                            CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_keys::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                 execplan::CalpontSystemCatalog::ColType& type)
{
  const string_view js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  IntType keySize = 0;
  string ret;
  json_engine_t jsEg;
  initJSEngine(jsEg, getCharset(fp[0]), js);

  if (fp.size() > 1)
  {
    if (!path.parsed && parseJSPath(path, row, fp[1], false))
      goto error;

    if (locateJSPath(jsEg, path))
      goto error;
  }

  if (json_read_value(&jsEg))
    goto error;

  if (jsEg.value_type != JSON_VALUE_OBJECT)
    goto error;

  ret.append("[");
  while (json_scan_next(&jsEg) == 0 && jsEg.state != JST_OBJ_END)
  {
    const uchar *keyStart, *keyEnd;
    int keyLen;

    switch (jsEg.state)
    {
      case JST_KEY:
        keyStart = jsEg.s.c_str;
        do
        {
          keyEnd = jsEg.s.c_str;
        } while (json_read_keyname_chr(&jsEg) == 0);

        if (unlikely(jsEg.s.error))
          goto error;

        keyLen = (int)(keyEnd - keyStart);

        if (!checkKeyInList(ret, keyStart, keyLen))
        {
          if (keySize > 0)
            ret.append(", ");
          ret.append("\"");
          ret.append((const char*)keyStart, keyLen);
          ret.append("\"");
          keySize++;
        }
        break;
      case JST_OBJ_START:
      case JST_ARRAY_START:
        if (json_skip_level(&jsEg))
          break;
        break;
      default: break;
    }
  }

  if (unlikely(!jsEg.s.error))
  {
    ret.append("]");
    return ret;
  }

error:
  isNull = true;
  return "";
}
}  // namespace funcexp
