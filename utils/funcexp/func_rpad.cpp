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
* $Id: func_rpad.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
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

namespace funcexp
{
CalpontSystemCatalog::ColType Func_rpad::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
    // operation type is not used by this functor
    return fp[0]->data()->resultType();
}


std::string Func_rpad::getStrVal(rowgroup::Row& row,
                                 FunctionParm& fp,
                                 bool& isNull,
                                 execplan::CalpontSystemCatalog::ColType&)
{
    unsigned i;
    // The number of characters (not bytes) in our input str.
    // Not all of these are necessarily significant. We need to search for the
    // NULL terminator to be sure.
    size_t strwclen;
    // this holds the number of characters (not bytes) in our pad str.
    size_t padwclen;

    // The original string
    const string& tstr = fp[0]->data()->getStrVal(row, isNull);

    // The result length in number of characters
    size_t len = 0;

    switch (fp[1]->data()->resultType().colDataType)
    {
        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        {
            len = fp[1]->data()->getIntVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            len = fp[1]->data()->getUintVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            double value = fp[1]->data()->getDoubleVal(row, isNull);

            if (value > 0)
                value += 0.5;
            else if (value < 0)
                value -= 0.5;
            else if (value < 0)
                value -= 0.5;

            int64_t ret = (int64_t) value;

            if (value > (double) numeric_limits<int64_t>::max())
                ret = numeric_limits<int64_t>::max();
            else if (value < (double) (numeric_limits<int64_t>::min() + 2))
                ret = numeric_limits<int64_t>::min() + 2; // IDB min for bigint

            len = ret;
        }
        break;

        default:
        {
            len = fp[1]->data()->getIntVal(row, isNull);
        }
    }

    if (len < 1)
        return "";

    // The pad characters.
    const string& pad = fp[2]->data()->getStrVal(row, isNull);

    if (isNull)
        return "";

    // Rather than calling the wideconvert functions with a null buffer to
    // determine the size of buffer to allocate, we can be sure the wide
    // char string won't be longer than:
    strwclen = tstr.length(); // a guess to start with. This will be >= to the real count.
    int alen = len;

    if (strwclen > len)
        alen = strwclen;

    int bufsize = (alen + 1) * sizeof(wchar_t);

    // Convert to wide characters. Do all further work in wide characters
    wchar_t* wcbuf = (wchar_t*)alloca(bufsize);
    strwclen = utf8::idb_mbstowcs(wcbuf, tstr.c_str(), strwclen + 1);

    unsigned int strSize = strwclen;    // The number of significant characters
    const wchar_t* pWChar = wcbuf;

    for (i = 0; *pWChar != '\0' && i < strwclen; ++pWChar, ++i)
    {
    }

    strSize = i;

    // If the incoming str is exactly the len of the result str,
    // return the original
    if (strSize == len)
    {
        return tstr;
    }

    // If the incoming str is too big for the result str
    // truncate the widechar buffer and return as a string
    if (strSize > len)
    {
        // Trim the excess length of the buffer
        wstring trimmed = wstring(wcbuf, len);
        return utf8::wstring_to_utf8(trimmed.c_str());
    }

    // This is the case where there's room to pad.

    // Convert the pad string to wide
    padwclen = pad.length();  // A guess to start.
    int padbufsize = (padwclen + 1) * sizeof(wchar_t);
    wchar_t* wcpad = (wchar_t*)alloca(padbufsize);
    size_t padlen = utf8::idb_mbstowcs(wcpad, pad.c_str(), padwclen + 1);

    // How many chars do we need?
    unsigned int padspace = len - strSize;

    // Fill in the back of the buffer
    wchar_t* firstpadchar = wcbuf + strSize;

    for (wchar_t* pch = wcbuf; pch < wcbuf + len && padlen > 0;)
    {
        // Truncate the number of fill chars if running out of space
        if (padlen > padspace)
        {
            padlen = padspace;
        }

        // Move the fill chars to buffer
        for (wchar_t* padchar = wcpad; padchar < wcpad + padlen; ++padchar)
        {
            *firstpadchar++ = *padchar;
        }

        padspace -= padlen;
        pch += padlen;
    }

    wstring padded = wstring(wcbuf, len);

    // Bug 5110 : strings were getting truncated since enough bytes not allocated.
    return utf8::wstring_to_utf8(padded.c_str());
}


} // namespace funcexp
// vim:ts=4 sw=4:

