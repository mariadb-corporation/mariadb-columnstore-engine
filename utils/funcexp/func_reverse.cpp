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
#include "utils_utf8.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#define STRCOLL_ENH__

namespace
{

void reverse( wchar_t* start, wchar_t* end )
{
    while ( start < end )
    {
        wchar_t c = *start;
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
    wstring wstr = utf8::utf8_to_wstring(str);
    int wlen = wstr.length();
    wchar_t* pbuf = new wchar_t[wlen + 1];
    wcsncpy(pbuf, wstr.c_str(), wlen);
    pbuf[wlen] = 0;
    wchar_t* end = pbuf + wlen - 1;
    reverse(pbuf, end);
    wstring wrstr = pbuf;
    string rstr = utf8::wstring_to_utf8(wrstr);
    delete [] pbuf;
    return rstr;
}


} // namespace funcexp
// vim:ts=4 sw=4:

