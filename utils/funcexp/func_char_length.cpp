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
 * $Id: func_char_length.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "dataconvert.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"

using namespace logging;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_char_length::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

int64_t Func_char_length::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& op_ct)
{
  CalpontSystemCatalog::ColDataType type = parm[0]->data()->resultType().colDataType;

  switch (type)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    case execplan::CalpontSystemCatalog::VARCHAR:  // including CHAR
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      const string& tstr = parm[0]->data()->getStrVal(row, isNull);
      if (isNull)
        return 0;
      const char* b = tstr.c_str();
      const char* e = tstr.c_str() + tstr.length();
      return (int64_t)parm[0]->data()->resultType().getCharset()->numchars(b, e);
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      string date = dataconvert::DataConvert::dateToString(parm[0]->data()->getDateIntVal(row, isNull));
      return (int64_t)date.size();
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      string date =
          dataconvert::DataConvert::datetimeToString(parm[0]->data()->getDatetimeIntVal(row, isNull));
      return (int64_t)date.size();
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      string date = dataconvert::DataConvert::timestampToString(
          parm[0]->data()->getTimestampIntVal(row, isNull), op_ct.getTimeZone());
      return (int64_t)date.size();
    }

    case execplan::CalpontSystemCatalog::TIME:
    {
      string date = dataconvert::DataConvert::timeToString(parm[0]->data()->getTimeIntVal(row, isNull));
      return (int64_t)date.size();
    }

    default:
    {
      std::ostringstream oss;
      oss << "char_length: datatype of " << execplan::colDataTypeToString(type);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  return 0;
}

}  // namespace funcexp
// vim:ts=4 sw=4:
