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
* $Id: func_lcase.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
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

class to_lower
{
public:
    char operator() (char c) const            // notice the return type
    {
        return tolower(c);
    }
};

namespace funcexp
{

CalpontSystemCatalog::ColType Func_lcase::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
    // operation type is not used by this functor
    return fp[0]->data()->resultType();
}

std::string Func_lcase::getStrVal(rowgroup::Row& row,
                                  FunctionParm& fp,
                                  bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType&)
{
//	string str = fp[0]->data()->getStrVal(row, isNull);

//	transform (str.begin(), str.end(), str.begin(), to_lower());

    const string& tstr = fp[0]->data()->getStrVal(row, isNull);

    if (isNull)
        return "";

    size_t strwclen = utf8::idb_mbstowcs(0, tstr.c_str(), 0) + 1;
    wchar_t* wcbuf = new wchar_t[strwclen];
    strwclen = utf8::idb_mbstowcs(wcbuf, tstr.c_str(), strwclen);
    wstring wstr(wcbuf, strwclen);

    for (uint32_t i = 0; i < strwclen; i++)
        wstr[i] = std::towlower(wstr[i]);

    size_t strmblen = utf8::idb_wcstombs(0, wstr.c_str(), 0) + 1;
    char* outbuf = new char[strmblen];
    strmblen = utf8::idb_wcstombs(outbuf, wstr.c_str(), strmblen);
    std::string ret(outbuf, strmblen);
    delete [] outbuf;
    delete [] wcbuf;
    return ret;
}


} // namespace funcexp
// vim:ts=4 sw=4:

