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
* $Id: func_reverse.cpp 2477 2011-04-01 16:07:35Z rdempsey $
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

#include "joblisttypes.h"
using namespace joblist;

namespace
{

void reverse( char* start, char* end )
{
    while ( start < end )
    {
        char c = *start;
        *start++ = *end;
        *end-- = c;
    }
}

}

namespace funcexp
{

CalpontSystemCatalog::ColType Func_reverse::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
    // operation type is not used by this functor
    return fp[0]->data()->resultType();
}

std::string Func_reverse::getStrVal(rowgroup::Row& row,
                                    FunctionParm& fp,
                                    bool& isNull,
                                    execplan::CalpontSystemCatalog::ColType&)
{
	string str;
    stringValue(fp[0], row, isNull, str);

    // We used to reverse in the string buffer, but that doesn't
    // work for all compilers as some re-use the buffer on simple
    // string assignment and implement a ref-count. Reversing in the
    // string buffer has the affect of reversing all strings from
    // which this one derived.
    int len = str.length();
    char* pbuf = new char[len + 1];
    strncpy(pbuf, str.c_str(), len);
    pbuf[len] = 0;
    char* end = pbuf + len - 1;
    reverse(pbuf, end);
    string rstr = pbuf;
    delete [] pbuf;
    return rstr;
}


} // namespace funcexp
// vim:ts=4 sw=4:

