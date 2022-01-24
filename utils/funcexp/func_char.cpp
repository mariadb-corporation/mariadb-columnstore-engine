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
 * $Id: func_char.cpp 3931 2013-06-21 20:17:28Z bwilkinson $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_str.h"
#include "funchelpers.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include <myisampack.h>  // min_intXstore()

#include "vlarray.h"

namespace
{
inline size_t getChar(int32_t num, char*& buf)
{
  char tmp[4];
  size_t numBytes = 0;
  if (num & 0xFF000000L)
  {
    mi_int4store(tmp, num);
    numBytes = 4;
  }
  else if (num & 0xFF0000L)
  {
    mi_int3store(tmp, num);
    numBytes = 3;
  }
  else if (num & 0xFF00L)
  {
    mi_int2store(tmp, num);
    numBytes = 2;
  }
  else
  {
    *((int8_t*)buf) = num;
    ++buf;
    return 1;
  }
  memcpy(buf, tmp, numBytes);
  buf += numBytes;
  return numBytes;
}

}  // namespace

namespace funcexp
{
CalpontSystemCatalog::ColType Func_char::operationType(FunctionParm& fp,
                                                       CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

string Func_char::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& ct)
{
  const int BUF_SIZE = 4 * parm.size();
  utils::VLArray<char, 1024> buf(BUF_SIZE);
  buf[0] = 0;
  char* pBuf = buf;
  CHARSET_INFO* cs = ct.getCharset();
  int32_t value = 0;
  int32_t numBytes = 0;
  for (uint32_t i = 0; i < parm.size(); ++i)
  {
    ReturnedColumn* rc = (ReturnedColumn*)parm[i]->data();

    switch (rc->resultType().colDataType)
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
      {
        value = rc->getIntVal(row, isNull);
      }
      break;

      case execplan::CalpontSystemCatalog::VARCHAR:  // including CHAR'
      case execplan::CalpontSystemCatalog::CHAR:
      case execplan::CalpontSystemCatalog::TEXT:
      case execplan::CalpontSystemCatalog::DOUBLE:
      case execplan::CalpontSystemCatalog::UDOUBLE:
      case execplan::CalpontSystemCatalog::FLOAT:
      case execplan::CalpontSystemCatalog::UFLOAT:
      {
        double vf = std::round(rc->getDoubleVal(row, isNull));
        value = (int32_t)vf;
      }
      break;

      case execplan::CalpontSystemCatalog::DECIMAL:
      case execplan::CalpontSystemCatalog::UDECIMAL:
      {
        value = static_cast<int32_t>(rc->getDecimalVal(row, isNull).toSInt64Round());
      }
      break;

      case execplan::CalpontSystemCatalog::DATE:
      case execplan::CalpontSystemCatalog::DATETIME:
      case execplan::CalpontSystemCatalog::TIMESTAMP:
      {
        continue;  // Dates are ignored
      }
      break;

      default:
      {
        value = 0;
      }
    }

    if (isNull)
      continue;

    numBytes += getChar(value, pBuf);
  }

  isNull = false;
  /* Check whether we got a well-formed string */
  MY_STRCOPY_STATUS status;
  int32_t actualBytes = cs->well_formed_char_length(buf, buf + numBytes, numBytes, &status);

  if (UNLIKELY(actualBytes < numBytes))
  {
    numBytes = actualBytes;
    ostringstream os;
    os << "Invalid character string for " << cs->cs_name.str << ": value = " << hex << buf + actualBytes;
    logging::Message::Args args;
    logging::Message message(9);
    args.add(os.str());
    logging::LoggingID logid(28);  // Shows as PrimProc, which may not be correct in all cases
    logging::Logger logger(logid.fSubsysID);
    logger.logMessage(logging::LOG_TYPE_WARNING, message, logid);
    // TODO: push warning to client
  }
  std::string ret(buf, numBytes);
  return ret;
}

}  // namespace funcexp
// vim:ts=4 sw=4:
