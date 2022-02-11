/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/****************************************************************************
 * $Id: func_find_in_set.cpp 2675 2011-06-22 04:58:07Z chao $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include <boost/tokenizer.hpp>
using namespace boost;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "dataconvert.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_find_in_set::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_find_in_set::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& op_ct)
{
  const string& searchStr = parm[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return 0;

  const string& setString = parm[1]->data()->getStrVal(row, isNull);
  if (isNull)
    return 0;

  if (searchStr.find(",") != string::npos)
    return 0;

  if (setString.length() < searchStr.length())
    return 0;

  CHARSET_INFO* cs = op_ct.getCharset();

  my_wc_t wc = 0;
  const char* str_begin = setString.c_str();
  const char* str_end = setString.c_str();
  const char* real_end = str_end + setString.length();
  const char* find_str = searchStr.c_str();
  size_t find_str_len = searchStr.length();
  int position = 0;
  static const char separator = ',';
  while (1)
  {
    int symbol_len;
    if ((symbol_len = cs->mb_wc(&wc, (uchar*)str_end, (uchar*)real_end)) > 0)
    {
      const char* substr_end = str_end + symbol_len;
      bool is_last_item = (substr_end == real_end);
      bool is_separator = (wc == (my_wc_t)separator);
      if (is_separator || is_last_item)
      {
        position++;
        if (is_last_item && !is_separator)
          str_end = substr_end;
        if (!cs->strnncoll(str_begin, (size_t)(str_end - str_begin), find_str, find_str_len))
          return (int64_t)position;
        else
          str_begin = substr_end;
      }
      str_end = substr_end;
    }
    else if (str_end - str_begin == 0 && find_str_len == 0 && wc == (my_wc_t)separator)
      return (longlong)++position;
    else
      return 0;
  }
  return 0;
}

double Func_find_in_set::getDoubleVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& ct)
{
  return (double)getIntVal(row, parm, isNull, ct);
}

string Func_find_in_set::getStrVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& ct)
{
  return intToString(getIntVal(row, parm, isNull, ct));
}

execplan::IDB_Decimal Func_find_in_set::getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                                      execplan::CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal decimal;
  decimal.value = getIntVal(row, fp, isNull, op_ct);
  decimal.scale = op_ct.scale;
  return decimal;
}

}  // namespace funcexp
// vim:ts=4 sw=4:
