#include <string_view>
using namespace std;

#include "functor_json.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "jsonhelpers.h"
using namespace funcexp::helpers;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_json_format::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

string Func_json_format::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& type)
{
  const auto& js = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";

  int tabSize = 4;

  if (fmt == DETAILED)
  {
    if (fp.size() > 1)
    {
      tabSize = fp[1]->data()->getIntVal(row, isNull);
      if (isNull)
        return "";

      if (tabSize < 0)
        tabSize = 0;
      else if (tabSize > TAB_SIZE_LIMIT)
        tabSize = TAB_SIZE_LIMIT;
    }
  }

  json_engine_t jsEg;

#if MYSQL_VERSION_ID >= 120100
  int jsEg_stack [JSON_DEPTH_LIMIT];

  mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM | MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE,
                              &jsEg.stack, sizeof(int), &jsEg_stack,
                              JSON_DEPTH_LIMIT, 0, MYF(0));
#endif

  initJSEngine(jsEg, getCharset(fp[0]), js);

  string ret;
  if (doFormat(&jsEg, ret, fmt, tabSize))
  {
    isNull = true;
    return "";
  }

  isNull = false;
  return ret;
}
}  // namespace funcexp
