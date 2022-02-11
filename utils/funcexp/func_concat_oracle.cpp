/* Copyright (C) 2021 MariaDB Corporation

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
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "dataconvert.h"
using namespace dataconvert;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_concat_oracle::operationType(FunctionParm& fp,
                                                                CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

// Returns the string that results from concatenating the arguments.
// concat_oracle() returns NULL all arguments are NULL.
// single arguments null is replaced by "".
//
string Func_concat_oracle::getStrVal(Row& row, FunctionParm& parm, bool& isNull,
                                     CalpontSystemCatalog::ColType&)
{
  string ret;
  string tmp;
  stringValue(parm[0], row, isNull, ret);
  // Oracle Mode should replace NULL with "" unless all values are NULL
  if (isNull)
  {
    ret = "";
    isNull = false;
  }
  // TODO: do a better job of cutting down the number re-allocations.
  // look at Item_func_concat::realloc_result for ideas and use
  // std::string:resize() appropriatly.
  for (unsigned int id = 1; id < parm.size(); id++)
  {
    stringValue(parm[id], row, isNull, tmp);
    if (isNull)
    {
      tmp = "";
      isNull = false;
    }
    ret.append(tmp);
  }

  return ret;
}

}  // namespace funcexp
// vim:ts=4 sw=4:
