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
 * $Id: func_crc32.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
 *
 *
 ****************************************************************************/

#include <unistd.h>
#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#ifndef _MSC_VER
#include "zlib.h"
#endif

namespace funcexp
{
CalpontSystemCatalog::ColType Func_crc32::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_crc32::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                              CalpontSystemCatalog::ColType& ct)
{
  unsigned crc = 0;
  switch (parm.size())
  {
    default: isNull = true; return 0;
    case 1: break;
    case 2:
      crc = static_cast<unsigned>(parm[0]->data()->getIntVal(row, isNull));
      if (isNull)
        return 0;
  }
  const string& b = parm[parm.size() - 1]->data()->getStrVal(row, isNull);
  if (isNull)
    return 0;
  return crc32(crc, reinterpret_cast<const uint8_t*>(b.data()), b.size());
}

}  // namespace funcexp
// vim:ts=4 sw=4:
