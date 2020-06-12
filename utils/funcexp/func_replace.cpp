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
* $Id: func_replace.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
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

#include "collation.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_replace::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
    // operation type is not used by this functor
    return fp[0]->data()->resultType();
}


std::string Func_replace::getStrVal(rowgroup::Row& row,
                                    FunctionParm& fp,
                                    bool& isNull,
                                    execplan::CalpontSystemCatalog::ColType& ct)
{
    CHARSET_INFO* cs = ct.getCharset();

    const string& str = fp[0]->data()->getStrVal(row, isNull);
    if (isNull)
        return "";
    size_t strLen = str.length();
    
    const string& fromstr = fp[1]->data()->getStrVal(row, isNull);
    if (isNull)
        return "";
    if (fromstr.length() == 0)
        return str;
    size_t fromLen = fromstr.length();
    
    const string& tostr = fp[2]->data()->getStrVal(row, isNull);
    if (isNull)
        return "";
    size_t toLen = tostr.length();

    bool binaryCmp = (cs->state & MY_CS_BINSORT) || !cs->use_mb();
    string newstr;
    size_t pos = 0;
    if (binaryCmp)
    {
        uint32_t i = 0;
        pos = str.find(fromstr);
        if (pos == string::npos)
            return str;
        
        // Count the number of fromstr in strend
        int count = 0;
        do
        {
            ++count;
            pos = str.find(fromstr, pos + fromLen);
        }
        while (pos != string::npos);
        
        newstr.reserve(strLen + (count * ((int)toLen - (int)fromLen)) + 1);
        
        // Now move the stuff into newstr
        do
        {
            if (pos > i)
                newstr = newstr + str.substr(i, pos - i);

            newstr = newstr + tostr;
            i = pos + fromLen;
            pos = str.find(fromstr, i);
        }
        while (pos != string::npos);
        
        newstr = newstr + str.substr(i, string::npos);
    }
    else
    {
        // UTF
        const char* src = str.c_str();
        const char* srcEnd = src + strLen;
        const char* srchEnd = srcEnd - fromLen + 1;
        const char* from = fromstr.c_str();
        const char* fromEnd = from + fromLen;
        const char* to = tostr.c_str();
        char* ptr = const_cast<char*>(src);
        char *i,*j;
        size_t count = 10; // Some arbitray number to reserve some space to start.
        size_t growlen = count * ((int)toLen - (int)fromLen);
        newstr.reserve(strLen + (count * growlen) + 1); 
        size_t maxsize = newstr.max_size();
        uint32_t l;

        // We don't know where byte patterns might match so
        // we start at the beginning of the string and move forward
        // one character at a time until we find a match. Then we can
        // move the src bytes and add in the to bytes,then try again.
        while (ptr < srchEnd)
        {
            bool found = false;
            if (*ptr == *from)  // If the first byte matches, maybe we have a match
            {
                // Do a byte by byte compare of src at that spot against from
                i = ptr + 1; 
                j = const_cast<char*>(from) + 1;
                found = true;
                while (j != fromEnd)
                {
                  if (*i++ != *j++)
                  {
                      found = false;
                      break;
                  }
                }
            }
            if (found)
            {
                if (ptr < i)
                {
                    int mvsize = i - ptr;
                    if (newstr.length() + mvsize + toLen < maxsize)
                    {
                        // We need a re-alloc
                        newstr.reserve(maxsize + growlen);
                        growlen *= 2;
                    }
                    newstr.append(ptr, mvsize);
                    ptr += mvsize;
                }
                newstr.append(to, toLen);
                ptr += toLen;
            }
            else
            {
                // move to the next character
                if ((l = my_ismbchar(cs, ptr, srcEnd))) // returns the number of bytes in the leading char or zero if one byte
                    ptr += l;
                else
                    ++ptr;
            }
        }
    }

    return newstr;
}


} // namespace funcexp
// vim:ts=4 sw=4:

