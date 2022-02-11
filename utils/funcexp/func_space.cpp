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
CalpontSystemCatalog::ColType Func_space::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

std::string Func_space::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& op_ct)
{
  CalpontSystemCatalog::ColDataType ct = fp[0]->data()->resultType().colDataType;

  // Int representation of temporal types can be a very large value,
  // so exit early if this is the case
  if (ct == CalpontSystemCatalog::DATE || ct == CalpontSystemCatalog::DATETIME ||
      ct == CalpontSystemCatalog::TIMESTAMP || ct == CalpontSystemCatalog::TIME)
  {
    isNull = true;
    return "";
  }

  int64_t count = fp[0]->data()->getIntVal(row, isNull);

  if (isNull || count < 1)
    return "";

  string result(count, ' ');

  return result;
}

}  // namespace funcexp
// vim:ts=4 sw=4:
