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
* $Id: func_strcmp.cpp 2477 2011-04-01 16:07:35Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "utils_utf8.h"
using namespace funcexp;

#include "collation.h"

// Because including my_sys.h in a Columnstore header causes too many conflicts
struct charset_info_st;
typedef const struct charset_info_st CHARSET_INFO;

namespace funcexp
{

CalpontSystemCatalog::ColType Func_strcmp::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
    // operation type is not used by this functor
    //return fp[0]->data()->resultType();
    return resultType;
}

int64_t Func_strcmp::getIntVal(rowgroup::Row& row,
                               FunctionParm& fp,
                               bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& type)
{
    CHARSET_INFO* cs = fp[0]->data()->resultType().getCharset();
    const string& str = fp[0]->data()->getStrVal(row, isNull);
    const string& str1 = fp[1]->data()->getStrVal(row, isNull);

    int ret = cs->strnncollsp(str.c_str(), str.length(), str1.c_str(), str1.length());
    // mysql's strcmp returns only -1, 0, and 1
    return (ret < 0 ? -1 : (ret > 0 ? 1 : 0));
}


std::string Func_strcmp::getStrVal(rowgroup::Row& row,
                                   FunctionParm& fp,
                                   bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& type)
{
    uint64_t val = getIntVal(row, fp, isNull, type);

    if (val > 0)
        return string("1");
    else if (val < 0)
        return string("-1");
    else
        return string("0");
}

} // namespace funcexp
// vim:ts=4 sw=4:

