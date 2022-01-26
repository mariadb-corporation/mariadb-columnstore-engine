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
 * $Id: func_concat_ws.cpp 3714 2013-04-18 16:29:25Z bpaul $
 *
 *
 ****************************************************************************/

#include <string>
using namespace std;

#include "functor_str.h"
#include "functioncolumn.h"
#include "utils_utf8.h"  // idb_mbstowcs()
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_concat_ws::operationType(FunctionParm& fp,
                                                            CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

string Func_concat_ws::getStrVal(Row& row, FunctionParm& parm, bool& isNull,
                                 CalpontSystemCatalog::ColType& type)
{
  string delim;
  stringValue(parm[0], row, isNull, delim);
  if (isNull)
    return "";

    // TODO: I don't think we need wide chars here.
    // Concatenation works without see Server implementation.
#if 0    
    wstring wstr;
    size_t strwclen = utf8::idb_mbstowcs(0, delim.c_str(), 0) + 1;
    wchar_t* wcbuf = new wchar_t[strwclen];
    strwclen = utf8::idb_mbstowcs(wcbuf, delim.c_str(), strwclen);
    wstring wdelim(wcbuf, strwclen);

    for ( unsigned int id = 1 ; id < parm.size() ; id++)
    {
		string tstr;
        stringValue(parm[id], row, isNull, tstr); 
        if (isNull)
        {
            isNull = false;
            continue;
        }

        if (!wstr.empty())
            wstr += wdelim;

        size_t strwclen1 = utf8::idb_mbstowcs(0, tstr.c_str(), 0) + 1;
        wchar_t* wcbuf1 = new wchar_t[strwclen1];
        strwclen1 = utf8::idb_mbstowcs(wcbuf1, tstr.c_str(), strwclen1);
        wstring str1(wcbuf1, strwclen1);
        wstr += str1;
        delete [] wcbuf1;
    }

    size_t strmblen = utf8::idb_wcstombs(0, wstr.c_str(), 0) + 1;
    char* outbuf = new char[strmblen];
    strmblen = utf8::idb_wcstombs(outbuf, wstr.c_str(), strmblen);

    if (strmblen == 0)
        isNull = true;
    else
        isNull = false;

    std::string ret(outbuf, strmblen);
    delete [] outbuf;
    delete [] wcbuf;
    return ret;
#endif
  string str;
  string tmp;
  for (uint32_t i = 1; i < parm.size(); i++)
  {
    stringValue(parm[i], row, isNull, tmp);
    if (isNull)
    {
      isNull = false;
      continue;
    }

    if (!str.empty())
      str += delim;

    // TODO: Work on string reallocation. Use std::string::resize() to
    // grab larger chunks in some intellegent manner.
    str += tmp;
  }

  if (str.empty())
    isNull = true;
  else
    isNull = false;

  return str;
}

}  // namespace funcexp
// vim:ts=4 sw=4:
