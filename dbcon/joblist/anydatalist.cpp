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

// $Id: anydatalist.cpp 9655 2013-06-25 23:08:13Z xlou $

#include "elementtype.h"

namespace joblist
{
//
//...Create an index where we store a manipulator flag (for each stream)
//...that controls the inclusion of a datalist's OID in stream operations.
//
static const int showOidInDataList_Index = std::ios_base::xalloc();

std::ostream& operator<<(std::ostream& oss, const AnyDataListSPtr& dl)
{
  bool withOid = (oss.iword(showOidInDataList_Index) != 0);

  if (auto* dle = dl->rowGroupDL(); dle != nullptr)
  {
    if (withOid)
      oss << dle->OID() << " ";

    //...If this datalist is saved to disk, then include the saved
    //...element size in the printed information.
    std::ostringstream elemSizeStr;

    if (dle->useDisk())
    {
      elemSizeStr << "(" << dle->getDiskElemSize1st() << "," << dle->getDiskElemSize2nd() << ")";
    }

    oss << "(0x" << std::hex << (ptrdiff_t)dle << std::dec << elemSizeStr.str() << ")";
  }
  else
  {
    oss << "0 (0x0000 [0])";
  }

  return oss;
}

//
//...showOidInDL is a manipulator that enables the inclusion of the data-
//...list's OID in subsequent invocations of the AnyDataListSPtr output
//...stream operator.
//
std::ostream& showOidInDL(std::ostream& strm)
{
  strm.iword(showOidInDataList_Index) = true;
  return strm;
}

//
//...omitOidInDL is a manipulator that disables the inclusion of the data-
//...list's OID in subsequent invocations of the AnyDataListSPtr output
//...stream operator.
//
std::ostream& omitOidInDL(std::ostream& strm)
{
  strm.iword(showOidInDataList_Index) = false;
  return strm;
}

}  // namespace joblist
