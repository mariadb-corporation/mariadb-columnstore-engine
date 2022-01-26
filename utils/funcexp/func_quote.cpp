/* Copyright (C) 2019 MariaDB Corporation.

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

#include <string>
using namespace std;

#include "functor_str.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_quote::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

std::string Func_quote::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& op_ct)
{
  string str;

  stringValue(fp[0], row, isNull, str);

  if (isNull)
  {
    isNull = false;
    return "NULL";
  }

  if (str.empty())
    return "NULL";

  string result;
  result.reserve((str.size() * 1.3) + 2);

  result.push_back('\'');

  for (uint64_t i = 0; i < str.size(); i++)
  {
    switch (str[i])
    {
      case 0:
        result.push_back('\\');
        result.push_back('0');
        break;
      case '\032':
        result.push_back('\\');
        result.push_back('Z');
        break;
      case '\'':
      case '\\':
        result.push_back('\\');
        result.push_back(str[i]);
        break;
      default: result.push_back(str[i]); break;
    }
  }

  result.push_back('\'');

  return result;
}

}  // namespace funcexp
// vim:ts=4 sw=4:
