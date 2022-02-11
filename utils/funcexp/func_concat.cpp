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
 * $Id: func_concat.cpp 3918 2013-06-19 13:54:34Z bwilkinson $
 *
 *
 ****************************************************************************/

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
CalpontSystemCatalog::ColType Func_concat::operationType(FunctionParm& fp,
                                                         CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

// Returns the string that results from concatenating the arguments.
// concat() returns NULL if any argument is NULL.
//
string Func_concat::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType&)
{
  string ret;
  string tmp;
  stringValue(parm[0], row, isNull, ret);

  // TODO: do a better job of cutting down the number re-allocations.
  // look at Item_func_concat::realloc_result for ideas and use
  // std::string:resize() appropriatly.
  for (unsigned int id = 1; id < parm.size(); id++)
  {
    stringValue(parm[id], row, isNull, tmp);
    ret.append(tmp);
  }

  return ret;
}

}  // namespace funcexp
// vim:ts=4 sw=4:
