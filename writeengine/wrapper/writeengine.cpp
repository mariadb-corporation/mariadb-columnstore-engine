/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016-2021 MariaDB Corporation

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

// $Id: writeengine.cpp 4737 2013-08-14 20:45:46Z bwilkinson $

/** @writeengine.cpp
 *   A wrapper class for the write engine to write information to files
 */
#include <cmath>
#include <cstdlib>
#include <unistd.h>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <unordered_map>
using namespace std;

#include "joblisttypes.h"

#define WRITEENGINEWRAPPER_DLLEXPORT
#include "writeengine.h"
#undef WRITEENGINEWRAPPER_DLLEXPORT

#include "we_convertor.h"
#include "we_log.h"
#include "we_simplesyslog.h"
#include "we_config.h"
#include "we_bulkrollbackmgr.h"
#include "brm.h"
#include "stopwatch.h"
#include "we_colop.h"
#include "we_type.h"

#include "we_colopcompress.h"
#include "we_dctnrycompress.h"
#include "cacheutils.h"
#include "calpontsystemcatalog.h"
#include "we_simplesyslog.h"
using namespace cacheutils;
using namespace logging;
using namespace BRM;
using namespace execplan;
#include "IDBDataFile.h"
#include "IDBPolicy.h"
#include "MonitorProcMem.h"
using namespace idbdatafile;
#include "dataconvert.h"

#ifdef _MSC_VER
#define isnan _isnan
#endif

namespace WriteEngine
//#define PROFILE 1

#define RETURN_ON_ERROR_REPORT(statement)                                                              \
  do                                                                                                   \
  {                                                                                                    \
    int rc = (statement);                                                                              \
    if (rc != NO_ERROR)                                                                                \
    {                                                                                                  \
      cerr << "failed at " << __LINE__ << "function " << __FUNCTION__ << ", statement '" << #statement \
           << "', rc " << rc << endl;                                                                  \
      return rc;                                                                                       \
    }                                                                                                  \
  } while (0)

{
StopWatch timer;
using OidToIdxMap = std::unordered_map<OID, ColStructList::size_type>;

/**@brief WriteEngineWrapper Constructor
 */
WriteEngineWrapper::WriteEngineWrapper() : m_opType(NOOP)
{
  m_colOp[UN_COMPRESSED_OP] = new ColumnOpCompress0;
  m_dctnry[UN_COMPRESSED_OP] = new DctnryCompress0;

  m_colOp[COMPRESSED_OP_1] = new ColumnOpCompress1(/*comressionType=*/1);
  m_dctnry[COMPRESSED_OP_1] = new DctnryCompress1(/*compressionType=*/1);

  m_colOp[COMPRESSED_OP_2] = new ColumnOpCompress1(/*comressionType=*/3);
  m_dctnry[COMPRESSED_OP_2] = new DctnryCompress1(/*compressionType=*/3);
}

WriteEngineWrapper::WriteEngineWrapper(const WriteEngineWrapper& rhs) : m_opType(rhs.m_opType)
{
  m_colOp[UN_COMPRESSED_OP] = new ColumnOpCompress0;
  m_dctnry[UN_COMPRESSED_OP] = new DctnryCompress0;

  m_colOp[COMPRESSED_OP_1] = new ColumnOpCompress1(/*compressionType=*/1);
  m_dctnry[COMPRESSED_OP_1] = new DctnryCompress1(/*compressionType=*/1);

  m_colOp[COMPRESSED_OP_2] = new ColumnOpCompress1(/*compressionType=*/3);
  m_dctnry[COMPRESSED_OP_2] = new DctnryCompress1(/*compressionType=*/3);
}

/**@brief WriteEngineWrapper Constructor
 */
WriteEngineWrapper::~WriteEngineWrapper()
{
  delete m_colOp[UN_COMPRESSED_OP];
  delete m_dctnry[UN_COMPRESSED_OP];

  delete m_colOp[COMPRESSED_OP_1];
  delete m_dctnry[COMPRESSED_OP_1];

  delete m_colOp[COMPRESSED_OP_2];
  delete m_dctnry[COMPRESSED_OP_2];
}

/**@brief Perform upfront initialization
 */
/* static */ void WriteEngineWrapper::init(unsigned subSystemID)
{
  SimpleSysLog::instance()->setLoggingID(logging::LoggingID(subSystemID));
  Config::initConfigCache();
  BRMWrapper::getInstance();

  // Bug 5415 Add HDFS MemBuffer vs. FileBuffer decision logic.
  config::Config* cf = config::Config::makeConfig();
  //--------------------------------------------------------------------------
  // Memory overload protection. This setting will cause the process to die should
  // it, by itself, consume maxPct of total memory. Monitored in MonitorProcMem.
  // Only used at the express direction of Field Support.
  //--------------------------------------------------------------------------
  int maxPct = 0;  // disable by default
  string strMaxPct = cf->getConfig("WriteEngine", "MaxPct");

  if (strMaxPct.length() != 0)
    maxPct = cf->uFromText(strMaxPct);

  //--------------------------------------------------------------------------
  // MemoryCheckPercent. This controls at what percent of total memory be consumed
  // by all processes before we switch from HdfsRdwrMemBuffer to HdfsRdwrFileBuffer.
  // This is only used in Hdfs installations.
  //--------------------------------------------------------------------------
  int checkPct = 95;
  string strCheckPct = cf->getConfig("SystemConfig", "MemoryCheckPercent");

  if (strCheckPct.length() != 0)
    checkPct = cf->uFromText(strCheckPct);

  //--------------------------------------------------------------------------
  // If we're either HDFS, or maxPct is turned on, start the monitor thread.
  // Otherwise, we don't need it, so don't waste the resources.
  //--------------------------------------------------------------------------
  if (maxPct > 0 || IDBPolicy::useHdfs())
  {
    new boost::thread(utils::MonitorProcMem(maxPct, checkPct, subSystemID));
  }
}

/*@brief checkValid --Check input parameters are valid
 */
/***********************************************************
 * DESCRIPTION:
 *    Check input parameters are valid
 * PARAMETERS:
 *    colStructList - column struct list
 *    colValueList - column value list
 *    ridList - rowid list
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in the checking process
 ***********************************************************/
int WriteEngineWrapper::checkValid(const TxnID& txnid, const ColStructList& colStructList,
                                   const ColValueList& colValueList, const RIDList& ridList) const
{
  ColTupleList curTupleList;
  ColStructList::size_type structListSize;
  ColValueList::size_type valListSize;
  ColTupleList::size_type totalRow;

  if (colStructList.size() == 0)
    return ERR_STRUCT_EMPTY;

  structListSize = colStructList.size();
  valListSize = colValueList.size();

  if (structListSize != valListSize)
    return ERR_STRUCT_VALUE_NOT_MATCH;

  for (ColValueList::size_type i = 0; i < valListSize; i++)
  {
    curTupleList = static_cast<ColTupleList>(colValueList[i]);
    totalRow = curTupleList.size();

    if (ridList.size() > 0)
    {
      if (totalRow != ridList.size())
        return ERR_ROWID_VALUE_NOT_MATCH;
    }

  }  // end of for (int i = 0;

  return NO_ERROR;
}

/*@brief findSmallestColumn --Find the smallest column for this table
 */
/***********************************************************
 * DESCRIPTION:
 *    Find the smallest column for this table
 * PARAMETERS:
 *    lowColLen - returns smallest column width
 *    colId - returns smallest column id
 *    colStructList - column struct list
 * RETURN:
 *  void
 ***********************************************************/
void WriteEngineWrapper::findSmallestColumn(uint32_t& colId, ColStructList colStructList)
// MCOL-1675: find the smallest column width to calculate the RowID from so
// that all HWMs will be incremented by this operation
{
  int32_t lowColLen = 8192;
  for (uint32_t colIt = 0; colIt < colStructList.size(); colIt++)
  {
    if (colStructList[colIt].colWidth < lowColLen)
    {
      colId = colIt;
      lowColLen = colStructList[colId].colWidth;
      if (lowColLen == 1)
      {
        break;
      }
    }
  }
}

/** @brief Fetch values from arrays into VT-types references casting arrays to (element type) ET-typed arrays.
 *
 * There might be two arrays: one from which we write to buffer and one with old values written before.
 * One of these arrays can be absent because of, well, physical absence. Insertion does not have
 * values written before and deletion does not have values to write to.
 */
template <typename VT, typename ET>
void fetchNewOldValues(VT& value, VT& oldValue, const void* array, const void* oldArray, size_t i,
                       size_t totalNewRow)
{
  const ET* eArray = static_cast<const ET*>(array);
  const ET* oldEArray = static_cast<const ET*>(oldArray);
  if (eArray)
  {
    value = eArray[i < totalNewRow ? i : 0];
  }
  if (oldEArray)
  {
    oldValue = oldEArray[i];
  }
}

static bool updateBigRangeCheckForInvalidity(ExtCPInfo* maxMin, int128_t value, int128_t oldValue,
                                             const void* valArrayVoid, const void* oldValArrayVoid)
{
  if (!oldValArrayVoid)
  {  // insertion. we can update range directly. range will not become invalid here.
    maxMin->fCPInfo.bigMax =
        std::max(maxMin->fCPInfo.bigMax,
                 value);  // we update big range because int columns can be associated with decimals.
    maxMin->fCPInfo.bigMin = std::min(maxMin->fCPInfo.bigMin, value);
  }
  else if (!valArrayVoid)
  {  // deletion. we need to check old value only, is it on (or outside) range boundary.
    if (oldValue >= maxMin->fCPInfo.bigMax || oldValue <= maxMin->fCPInfo.bigMin)
    {
      maxMin->toInvalid();
      return true;  // no point working further.
    }
  }
  else if (valArrayVoid && oldValArrayVoid)
  {  // update. we need to check boundaries as in deletion and extend as in insertion.
     // check boundaries as in deletion accounting for possible extension.
    if ((oldValue <= maxMin->fCPInfo.bigMin && value > oldValue) ||
        (oldValue >= maxMin->fCPInfo.bigMax && value < oldValue))
    {  // we are overwriting value on the boundary with value that does not preserve or extend range.
      maxMin->toInvalid();
      return true;
    }
    maxMin->fCPInfo.bigMax = std::max(maxMin->fCPInfo.bigMax, value);
    maxMin->fCPInfo.bigMin = std::min(maxMin->fCPInfo.bigMin, value);
  }
  return false;
}

template <typename InternalType>
bool updateRangeCheckForInvalidity(ExtCPInfo* maxMin, InternalType value, InternalType oldValue,
                                   const void* valArrayVoid, const void* oldValArrayVoid)
{
  if (!oldValArrayVoid)
  {  // insertion. we can update range directly. range will not become invalid here.
    maxMin->fCPInfo.bigMax = std::max(
        maxMin->fCPInfo.bigMax,
        (int128_t)value);  // we update big range because int columns can be associated with decimals.
    maxMin->fCPInfo.bigMin = std::min(maxMin->fCPInfo.bigMin, (int128_t)value);
    maxMin->fCPInfo.max = std::max((InternalType)maxMin->fCPInfo.max, value);
    maxMin->fCPInfo.min = std::min((InternalType)maxMin->fCPInfo.min, value);
  }
  else if (!valArrayVoid)
  {  // deletion. we need to check old value only, is it on (or outside) range boundary.
    if (oldValue >= (InternalType)maxMin->fCPInfo.max || oldValue <= (InternalType)maxMin->fCPInfo.min)
    {
      maxMin->toInvalid();
      return true;  // no point working further.
    }
  }
  else if (valArrayVoid && oldValArrayVoid)
  {  // update. we need to check boundaries as in deletion and extend as in insertion.
     // check boundaries as in deletion accounting for possible extension.
    if ((oldValue <= (InternalType)maxMin->fCPInfo.min && value > oldValue) ||
        (oldValue >= (InternalType)maxMin->fCPInfo.max && value < oldValue))
    {  // we are overwriting value on the boundary with value that does not preserve or extend range.
      maxMin->toInvalid();
      return true;
    }
    maxMin->fCPInfo.bigMax = std::max(maxMin->fCPInfo.bigMax, (int128_t)value);
    maxMin->fCPInfo.bigMin = std::min(maxMin->fCPInfo.bigMin, (int128_t)value);
    maxMin->fCPInfo.max = std::max((InternalType)maxMin->fCPInfo.max, value);
    maxMin->fCPInfo.min = std::min((InternalType)maxMin->fCPInfo.min, value);
  }
  return false;
}

/**
 * There can be case with missing valArray (delete), missing oldValArray (insert) and when
 * both arrays are present (update).
 */
void WriteEngineWrapper::updateMaxMinRange(const size_t totalNewRow, const size_t totalOldRow,
                                           const execplan::CalpontSystemCatalog::ColType& cscColType,
                                           const ColType colType, const void* valArrayVoid,
                                           const void* oldValArrayVoid, ExtCPInfo* maxMin,
                                           bool canStartWithInvalidRange)
{
  if (!maxMin)
  {
    return;
  }
  if (colType == WR_CHAR)
  {
    maxMin->toInvalid();  // simple and wrong solution.
    return;
  }
  bool isUnsigned = false;  // TODO: should change with type.
  switch (colType)
  {
    case WR_UBYTE:
    case WR_USHORT:
    case WR_UINT:
    case WR_ULONGLONG:
    case WR_CHAR:
    {
      isUnsigned = true;
      break;
    }
    default:  // WR_BINARY is signed.
      break;
  }
  if (!canStartWithInvalidRange)
  {
    // check if range is invalid, we can't update it.
    if (maxMin->isInvalid())
    {
      return;
    }
  }
  if (colType == WR_CHAR)
  {
    idbassert(MAX_COLUMN_BOUNDARY == sizeof(uint64_t));  // have to check that - code below depends on that.
    if (!maxMin->isInvalid())
    {
      maxMin->fromToChars();
    }
  }
  size_t i;
  for (i = 0; i < totalOldRow; i++)
  {
    int64_t value = 0, oldValue = 0;
    uint64_t uvalue = 0, oldUValue = 0;
    int128_t bvalue = 0, oldBValue = 0;

    // fetching. May not assign value or oldValue variables when array is not present.
    switch (colType)
    {
      case WR_BYTE:
      {
        fetchNewOldValues<int64_t, int8_t>(value, oldValue, valArrayVoid, oldValArrayVoid, i, totalNewRow);
        break;
      }
      case WR_UBYTE:
      {
        fetchNewOldValues<uint64_t, uint8_t>(uvalue, oldUValue, valArrayVoid, oldValArrayVoid, i,
                                             totalNewRow);
        break;
      }
      case WR_SHORT:
      {
        fetchNewOldValues<int64_t, int16_t>(value, oldValue, valArrayVoid, oldValArrayVoid, i, totalNewRow);
        break;
      }
      case WR_USHORT:
      {
        fetchNewOldValues<uint64_t, uint16_t>(uvalue, oldUValue, valArrayVoid, oldValArrayVoid, i,
                                              totalNewRow);
        break;
      }
      case WR_MEDINT:
      case WR_INT:
      {
        fetchNewOldValues<int64_t, int>(value, oldValue, valArrayVoid, oldValArrayVoid, i, totalNewRow);
        break;
      }
      case WR_UMEDINT:
      case WR_UINT:
      {
        fetchNewOldValues<uint64_t, unsigned int>(uvalue, oldUValue, valArrayVoid, oldValArrayVoid, i,
                                                  totalNewRow);
        break;
      }
      case WR_LONGLONG:
      {
        fetchNewOldValues<int64_t, int64_t>(value, oldValue, valArrayVoid, oldValArrayVoid, i, totalNewRow);
        break;
      }
      case WR_ULONGLONG:
      {
        fetchNewOldValues<uint64_t, uint64_t>(uvalue, oldUValue, valArrayVoid, oldValArrayVoid, i,
                                              totalNewRow);
        break;
      }
      case WR_BINARY:
      {
        fetchNewOldValues<int128_t, int128_t>(bvalue, oldBValue, valArrayVoid, oldValArrayVoid, i,
                                              totalNewRow);
        break;
      }
      case WR_CHAR:
      {
        fetchNewOldValues<uint64_t, uint64_t>(uvalue, oldUValue, valArrayVoid, oldValArrayVoid, i,
                                              totalNewRow);
        // for characters (strings, actually), we fetched then in LSB order, on x86, at the very least.
        // this means most significant byte of the string, which is first, is now in LSB of uvalue/oldValue.
        // we must perform a conversion.
        uvalue = uint64ToStr(uvalue);
        oldValue = uint64ToStr(oldValue);
        break;
      }
      default: idbassert_s(0, "unknown WR type tag"); return;
    }
    if (maxMin->isBinaryColumn())
    {  // special case of wide decimals. They fit into int128_t range so we do not care about signedness.
      if (updateBigRangeCheckForInvalidity(maxMin, bvalue, oldBValue, valArrayVoid, oldValArrayVoid))
      {
        return;
      }
    }
    else if (isUnsigned)
    {
      if (updateRangeCheckForInvalidity(maxMin, uvalue, oldUValue, valArrayVoid, oldValArrayVoid))
      {
        return;
      }
    }
    else
    {
      if (updateRangeCheckForInvalidity(maxMin, value, oldValue, valArrayVoid, oldValArrayVoid))
      {
        return;
      }
    }
  }
  // the range will be kept.
  // to handle character columns properly we need to convert range values back to strings.
  if (colType == WR_CHAR)
  {
    maxMin->fromToChars();
  }
}
/*@convertValArray - Convert interface values to internal values
 */
/***********************************************************
 * DESCRIPTION:
 *    Convert interface values to internal values
 * PARAMETERS:
 *    cscColType -  CSC ColType struct list
 *    colStructList - column struct list
 *    colValueList - column value list
 * RETURN:
 *    none
 *    valArray - output value array
 *    nullArray - output null flag array
 ***********************************************************/
void WriteEngineWrapper::convertValArray(const size_t totalRow,
                                         const CalpontSystemCatalog::ColType& cscColType,
                                         const ColType colType, ColTupleList& curTupleList, void* valArray,
                                         bool bFromList)
{
  ColTuple curTuple;
  ColTupleList::size_type i;

  if (bFromList)
  {
    for (i = 0; i < curTupleList.size(); i++)
    {
      curTuple = curTupleList[i];
      convertValue(cscColType, colType, valArray, i, curTuple.data, true);
    }
  }
  else
  {
    for (i = 0; i < totalRow; i++)
    {
      convertValue(cscColType, colType, valArray, i, curTuple.data, false);
      curTupleList.push_back(curTuple);
    }
  }
}

/*
 * @brief Convert column value to its internal representation
 */
void WriteEngineWrapper::convertValue(const execplan::CalpontSystemCatalog::ColType& cscColType,
                                      ColType colType, void* value, boost::any& data)
{
  string curStr;
  int size;

  switch (colType)
  {
    case WriteEngine::WR_INT:
    case WriteEngine::WR_MEDINT:
      if (data.type() == typeid(int))
      {
        int val = boost::any_cast<int>(data);
        size = sizeof(int);
        memcpy(value, &val, size);
      }
      else
      {
        uint32_t val = boost::any_cast<uint32_t>(data);
        size = sizeof(uint32_t);
        memcpy(value, &val, size);
      }

      break;

    case WriteEngine::WR_UINT:
    case WriteEngine::WR_UMEDINT:
    {
      uint32_t val = boost::any_cast<uint32_t>(data);
      size = sizeof(uint32_t);
      memcpy(value, &val, size);
    }
    break;

    case WriteEngine::WR_VARBINARY:  // treat same as char for now
    case WriteEngine::WR_CHAR:
    case WriteEngine::WR_BLOB:
    case WriteEngine::WR_TEXT:
      curStr = boost::any_cast<string>(data);

      if ((int)curStr.length() > MAX_COLUMN_BOUNDARY)
        curStr = curStr.substr(0, MAX_COLUMN_BOUNDARY);

      memcpy(value, curStr.c_str(), curStr.length());
      break;

    case WriteEngine::WR_FLOAT:
    {
      float val = boost::any_cast<float>(data);

      // N.B.There is a bug in boost::any or in gcc where, if you store a nan, you will get back a nan,
      // but not necessarily the same bits that you put in. This only seems to be for float (double seems
      // to work).
      if (isnan(val))
      {
        uint32_t ti = joblist::FLOATNULL;
        float* tfp = (float*)&ti;
        val = *tfp;
      }

      size = sizeof(float);
      memcpy(value, &val, size);
    }
    break;

    case WriteEngine::WR_DOUBLE:
    {
      double val = boost::any_cast<double>(data);
      size = sizeof(double);
      memcpy(value, &val, size);
    }
    break;

    case WriteEngine::WR_SHORT:
    {
      short val = boost::any_cast<short>(data);
      size = sizeof(short);
      memcpy(value, &val, size);
    }
    break;

    case WriteEngine::WR_USHORT:
    {
      uint16_t val = boost::any_cast<uint16_t>(data);
      size = sizeof(uint16_t);
      memcpy(value, &val, size);
    }
    break;

    case WriteEngine::WR_BYTE:
    {
      char val = boost::any_cast<char>(data);
      size = sizeof(char);
      memcpy(value, &val, size);
    }
    break;

    case WriteEngine::WR_UBYTE:
    {
      uint8_t val = boost::any_cast<uint8_t>(data);
      size = sizeof(uint8_t);
      memcpy(value, &val, size);
    }
    break;

    case WriteEngine::WR_LONGLONG:
      if (data.type() == typeid(long long))
      {
        long long val = boost::any_cast<long long>(data);
        size = sizeof(long long);
        memcpy(value, &val, size);
      }
      else if (data.type() == typeid(long))
      {
        long val = boost::any_cast<long>(data);
        size = sizeof(long);
        memcpy(value, &val, size);
      }
      else
      {
        uint64_t val = boost::any_cast<uint64_t>(data);
        size = sizeof(uint64_t);
        memcpy(value, &val, size);
      }

      break;

    case WriteEngine::WR_ULONGLONG:
    {
      uint64_t val = boost::any_cast<uint64_t>(data);
      size = sizeof(uint64_t);
      memcpy(value, &val, size);
    }
    break;

    case WriteEngine::WR_TOKEN:
    {
      Token val = boost::any_cast<Token>(data);
      size = sizeof(Token);
      memcpy(value, &val, size);
    }
    break;

    case WriteEngine::WR_BINARY:
    {
      size = cscColType.colWidth;
      int128_t val = boost::any_cast<int128_t>(data);
      memcpy(value, &val, size);
    }
    break;

  }  // end of switch (colType)
} /*@convertValue -  The base for converting values */

/***********************************************************
 * DESCRIPTION:
 *    The base for converting values
 * PARAMETERS:
 *    colType - data type
 *    pos - array position
 *    data - value
 * RETURN:
 *    none
 ***********************************************************/
void WriteEngineWrapper::convertValue(const CalpontSystemCatalog::ColType& cscColType, const ColType colType,
                                      void* valArray, const size_t pos, boost::any& data, bool fromList)
{
  string curStr;

  if (fromList)
  {
    switch (colType)
    {
      case WriteEngine::WR_INT:
      case WriteEngine::WR_MEDINT:
        // here we assign value to an array element and update range.
        if (data.type() == typeid(long))
          ((int*)valArray)[pos] = static_cast<int>(boost::any_cast<long>(data));
        else if (data.type() == typeid(int))
          ((int*)valArray)[pos] = boost::any_cast<int>(data);
        else
        {  // this interesting part is for magic values like NULL or EMPTY (marks deleted elements).
          // we will not put these into range.
          ((int*)valArray)[pos] = boost::any_cast<uint32_t>(data);
        }

        break;

      case WriteEngine::WR_UINT:
      case WriteEngine::WR_UMEDINT: ((uint32_t*)valArray)[pos] = boost::any_cast<uint32_t>(data); break;

      case WriteEngine::WR_VARBINARY:  // treat same as char for now
      case WriteEngine::WR_CHAR:
      case WriteEngine::WR_BLOB:
      case WriteEngine::WR_TEXT:
        curStr = boost::any_cast<string>(data);

        if ((int)curStr.length() > MAX_COLUMN_BOUNDARY)
          curStr = curStr.substr(0, MAX_COLUMN_BOUNDARY);

        memcpy((char*)valArray + pos * MAX_COLUMN_BOUNDARY, curStr.c_str(), curStr.length());
        break;

        //            case WriteEngine::WR_LONG :   ((long*)valArray)[pos] =
        //            boost::any_cast<long>(curTuple.data);
        //                                          break;
      case WriteEngine::WR_FLOAT:
        ((float*)valArray)[pos] = boost::any_cast<float>(data);

        if (isnan(((float*)valArray)[pos]))
        {
          uint32_t ti = joblist::FLOATNULL;
          float* tfp = (float*)&ti;
          ((float*)valArray)[pos] = *tfp;
        }

        break;

      case WriteEngine::WR_DOUBLE: ((double*)valArray)[pos] = boost::any_cast<double>(data); break;

      case WriteEngine::WR_SHORT: ((short*)valArray)[pos] = boost::any_cast<short>(data); break;

      case WriteEngine::WR_USHORT:
        ((uint16_t*)valArray)[pos] = boost::any_cast<uint16_t>(data);
        break;

        //            case WriteEngine::WR_BIT:     ((bool*)valArray)[pos] = boost::any_cast<bool>(data);
        //                                          break;
      case WriteEngine::WR_BYTE: ((char*)valArray)[pos] = boost::any_cast<char>(data); break;

      case WriteEngine::WR_UBYTE: ((uint8_t*)valArray)[pos] = boost::any_cast<uint8_t>(data); break;

      case WriteEngine::WR_LONGLONG:
        if (data.type() == typeid(long long))
          ((long long*)valArray)[pos] = boost::any_cast<long long>(data);
        else if (data.type() == typeid(long))
          ((long long*)valArray)[pos] = (long long)boost::any_cast<long>(data);
        else
          ((long long*)valArray)[pos] = boost::any_cast<uint64_t>(data);

        break;

      case WriteEngine::WR_ULONGLONG: ((uint64_t*)valArray)[pos] = boost::any_cast<uint64_t>(data); break;

      case WriteEngine::WR_TOKEN: ((Token*)valArray)[pos] = boost::any_cast<Token>(data); break;

      case WriteEngine::WR_BINARY:
        size_t size = cscColType.colWidth;
        int128_t val = boost::any_cast<int128_t>(data);
        memcpy((uint8_t*)valArray + pos * size, &val, size);
        break;
    }  // end of switch (colType)
  }
  else
  {
    switch (colType)
    {
      case WriteEngine::WR_INT:
      case WriteEngine::WR_MEDINT: data = ((int*)valArray)[pos]; break;

      case WriteEngine::WR_UINT:
      case WriteEngine::WR_UMEDINT: data = ((uint64_t*)valArray)[pos]; break;

      case WriteEngine::WR_VARBINARY:  // treat same as char for now
      case WriteEngine::WR_CHAR:
      case WriteEngine::WR_BLOB:
      case WriteEngine::WR_TEXT:
        char tmp[10];
        memcpy(tmp, (char*)valArray + pos * 8, 8);
        curStr = tmp;
        data = curStr;
        break;

        //            case WriteEngine::WR_LONG :   ((long*)valArray)[pos] =
        //            boost::any_cast<long>(curTuple.data);
        //                                          break;
      case WriteEngine::WR_FLOAT: data = ((float*)valArray)[pos]; break;

      case WriteEngine::WR_DOUBLE: data = ((double*)valArray)[pos]; break;

      case WriteEngine::WR_SHORT: data = ((short*)valArray)[pos]; break;

      case WriteEngine::WR_USHORT:
        data = ((uint16_t*)valArray)[pos];
        break;

        //            case WriteEngine::WR_BIT:     data = ((bool*)valArray)[pos];
        //                                          break;
      case WriteEngine::WR_BYTE: data = ((char*)valArray)[pos]; break;

      case WriteEngine::WR_UBYTE: data = ((uint8_t*)valArray)[pos]; break;

      case WriteEngine::WR_LONGLONG: data = ((long long*)valArray)[pos]; break;

      case WriteEngine::WR_ULONGLONG: data = ((uint64_t*)valArray)[pos]; break;

      case WriteEngine::WR_TOKEN: data = ((Token*)valArray)[pos]; break;

      case WriteEngine::WR_BINARY: data = ((int128_t*)valArray)[pos]; break;

    }  // end of switch (colType)
  }    // end of if
}

/*@createColumn -  Create column files, including data and bitmap files
 */
/***********************************************************
 * DESCRIPTION:
 *    Create column files, including data and bitmap files
 * PARAMETERS:
 *    dataOid - column data file id
 *    bitmapOid - column bitmap file id
 *    colWidth - column width
 *    dbRoot   - DBRoot where file is to be located
 *    partition - Starting partition number for segment file path
 *     compressionType - compression type
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_EXIST if file exists
 *    ERR_FILE_CREATE if something wrong in creating the file
 ***********************************************************/
int WriteEngineWrapper::createColumn(const TxnID& txnid, const OID& dataOid,
                                     const CalpontSystemCatalog::ColDataType dataType, int dataWidth,
                                     uint16_t dbRoot, uint32_t partition, int compressionType)
{
  int rc;
  Column curCol;

  int compress_op = op(compressionType);
  m_colOp[compress_op]->initColumn(curCol);
  m_colOp[compress_op]->findTypeHandler(dataWidth, dataType);

  rc = m_colOp[compress_op]->createColumn(curCol, 0, dataWidth, dataType, WriteEngine::WR_CHAR, (FID)dataOid,
                                          dbRoot, partition);

  // This is optional, however, it's recommended to do so to free heap
  // memory if assigned in the future
  m_colOp[compress_op]->clearColumn(curCol);
  std::map<FID, FID> oids;

  if (rc == NO_ERROR)
    rc = flushDataFiles(NO_ERROR, txnid, oids);

  if (rc != NO_ERROR)
  {
    return rc;
  }

  RETURN_ON_ERROR(BRMWrapper::getInstance()->setLocalHWM(dataOid, partition, 0, 0));
  // @bug 281 : fix for bug 281 - Add flush VM cache to clear all write buffer
  // flushVMCache();
  return rc;
}

// BUG931
/**
 * @brief Fill column with default values
 */
int WriteEngineWrapper::fillColumn(const TxnID& txnid, const OID& dataOid,
                                   const CalpontSystemCatalog::ColType& colType, ColTuple defaultVal,
                                   const OID& refColOID,
                                   const CalpontSystemCatalog::ColDataType refColDataType, int refColWidth,
                                   int refCompressionType, bool isNULL, int compressionType,
                                   const string& defaultValStr, const OID& dictOid, bool autoincrement)
{
  int rc = NO_ERROR;
  Column newCol;
  Column refCol;
  ColType newColType;
  ColType refColType;
  boost::scoped_array<char> defVal(new char[MAX_COLUMN_BOUNDARY]);
  ColumnOp* colOpNewCol = m_colOp[op(compressionType)];
  ColumnOp* refColOp = m_colOp[op(refCompressionType)];
  Dctnry* dctnry = m_dctnry[op(compressionType)];
  colOpNewCol->initColumn(newCol);
  refColOp->initColumn(refCol);
  uint16_t dbRoot = 1;  // not to be used
  int newDataWidth = colType.colWidth;
  // Convert HWM of the reference column for the new column
  // Bug 1703,1705
  bool isToken = false;

  if (((colType.colDataType == CalpontSystemCatalog::VARCHAR) && (colType.colWidth > 7)) ||
      ((colType.colDataType == CalpontSystemCatalog::CHAR) && (colType.colWidth > 8)) ||
      (colType.colDataType == CalpontSystemCatalog::VARBINARY) ||
      (colType.colDataType == CalpontSystemCatalog::BLOB) ||
      (colType.colDataType == CalpontSystemCatalog::TEXT))
  {
    isToken = true;
  }

  Convertor::convertColType(colType.colDataType, colType.colWidth, newColType, isToken);

  // WIP
  // replace with isDictCol
  if (((refColDataType == CalpontSystemCatalog::VARCHAR) && (refColWidth > 7)) ||
      ((refColDataType == CalpontSystemCatalog::CHAR) && (refColWidth > 8)) ||
      (refColDataType == CalpontSystemCatalog::VARBINARY) ||
      (colType.colDataType == CalpontSystemCatalog::BLOB) ||
      (colType.colDataType == CalpontSystemCatalog::TEXT))
  {
    isToken = true;
  }

  newDataWidth = colOpNewCol->getCorrectRowWidth(colType.colDataType, colType.colWidth);
  // MCOL-1347 CS doubles the width for ALTER TABLE..ADD COLUMN
  if (colType.colWidth < 4 && colType.colDataType == CalpontSystemCatalog::VARCHAR)
  {
    newDataWidth >>= 1;
  }

  Convertor::convertColType(refColDataType, refColWidth, refColType, isToken);
  refColOp->setColParam(refCol, 0, refColOp->getCorrectRowWidth(refColDataType, refColWidth), refColDataType,
                        refColType, (FID)refColOID, refCompressionType, dbRoot);
  colOpNewCol->setColParam(newCol, 0, newDataWidth, colType.colDataType, newColType, (FID)dataOid,
                           compressionType, dbRoot);
  // refColOp and colOpNewCol are the same ptr.
  colOpNewCol->findTypeHandler(newDataWidth, colType.colDataType);

  int size = sizeof(Token);

  if (newColType == WriteEngine::WR_TOKEN)
  {
    if (isNULL)
    {
      Token nullToken;
      memcpy(defVal.get(), &nullToken, size);
    }
    // Tokenization is done when we create dictionary file
  }
  else
  {
    // WIP
    convertValue(colType, newColType, defVal.get(), defaultVal.data);
  }

  if (rc == NO_ERROR)
    rc = colOpNewCol->fillColumn(txnid, newCol, refCol, defVal.get(), dctnry, refColOp, dictOid,
                                 colType.colWidth, defaultValStr, autoincrement);

  // flushing files is in colOp->fillColumn()

  return rc;
}

int WriteEngineWrapper::deleteRow(const TxnID& txnid, const vector<CSCTypesList>& colExtentsColType,
                                  vector<ColStructList>& colExtentsStruct, vector<void*>& colOldValueList,
                                  vector<RIDList>& ridLists, const int32_t tableOid)
{
  ColTuple curTuple;
  ColStruct curColStruct;
  CalpontSystemCatalog::ColType cscColType;
  DctnryStruct dctnryStruct;
  ColValueList colValueList;
  ColTupleList curTupleList;
  DctnryStructList dctnryStructList;
  DctnryValueList dctnryValueList;
  ColStructList colStructList;
  CSCTypesList cscColTypeList;
  int rc;
  string tmpStr("");
  vector<DctnryStructList> dctnryExtentsStruct;

  if (colExtentsStruct.size() == 0 || ridLists.size() == 0)
    return ERR_STRUCT_EMPTY;

  // set transaction id
  setTransId(txnid);
  unsigned numExtents = colExtentsStruct.size();

  for (unsigned extent = 0; extent < numExtents; extent++)
  {
    colStructList = colExtentsStruct[extent];
    cscColTypeList = colExtentsColType[extent];

    for (ColStructList::size_type i = 0; i < colStructList.size(); i++)
    {
      curTupleList.clear();
      curColStruct = colStructList[i];
      cscColType = cscColTypeList[i];
      Convertor::convertColType(&curColStruct);

      const uint8_t* emptyVal = m_colOp[op(curColStruct.fCompressionType)]->getEmptyRowValue(
          curColStruct.colDataType, curColStruct.colWidth);

      if (curColStruct.colWidth == datatypes::MAXDECIMALWIDTH)
        curTuple.data = *(int128_t*)emptyVal;
      else
        curTuple.data = *(int64_t*)emptyVal;

      curTupleList.push_back(curTuple);
      colValueList.push_back(curTupleList);

      dctnryStruct.dctnryOid = 0;
      dctnryStruct.fColPartition = curColStruct.fColPartition;
      dctnryStruct.fColSegment = curColStruct.fColSegment;
      dctnryStruct.fColDbRoot = curColStruct.fColDbRoot;
      dctnryStruct.columnOid = colStructList[i].dataOid;
      dctnryStructList.push_back(dctnryStruct);

      DctnryTuple dctnryTuple;
      DctColTupleList dctColTuples;
      dctnryTuple.sigValue = (unsigned char*)tmpStr.c_str();
      dctnryTuple.sigSize = tmpStr.length();
      dctnryTuple.isNull = true;
      dctColTuples.push_back(dctnryTuple);
      dctnryValueList.push_back(dctColTuples);
    }

    dctnryExtentsStruct.push_back(dctnryStructList);
  }

  // unfortunately I don't have a better way to instruct without passing too many parameters
  m_opType = DELETE;
  rc = updateColumnRec(txnid, colExtentsColType, colExtentsStruct, colValueList, colOldValueList, ridLists,
                       dctnryExtentsStruct, dctnryValueList, tableOid);
  m_opType = NOOP;

  return rc;
}

inline void allocateValArray(void*& valArray, ColTupleList::size_type totalRow, ColType colType, int colWidth)
{
  // MCS allocates 8 bytes even for CHARs smaller then 8 bytes.
  switch (colType)
  {
    case WriteEngine::WR_VARBINARY:  // treat same as char for now
    case WriteEngine::WR_CHAR:
    case WriteEngine::WR_BLOB:
    case WriteEngine::WR_TEXT: valArray = calloc(sizeof(char), totalRow * MAX_COLUMN_BOUNDARY); break;
    case WriteEngine::WR_TOKEN: valArray = calloc(sizeof(Token), totalRow); break;
    default: valArray = calloc(totalRow, colWidth); break;
  }
}

int WriteEngineWrapper::deleteBadRows(const TxnID& txnid, ColStructList& colStructs, RIDList& ridList,
                                      DctnryStructList& dctnryStructList)
{
  /*  Need to scan all files including dictionary store files to check whether there is any bad chunks
   *
   */
  int rc = 0;
  Column curCol;
  void* valArray = NULL;

  m_opType = DELETE;

  for (unsigned i = 0; i < colStructs.size(); i++)
  {
    ColumnOp* colOp = m_colOp[op(colStructs[i].fCompressionType)];
    unsigned needFixFiles = colStructs[i].tokenFlag ? 2 : 1;
    colOp->initColumn(curCol);

    for (unsigned j = 0; j < needFixFiles; j++)
    {
      if (j == 0)
      {
        colOp->setColParam(curCol, 0, colStructs[i].colWidth, colStructs[i].colDataType,
                           colStructs[i].colType, colStructs[i].dataOid, colStructs[i].fCompressionType,
                           colStructs[i].fColDbRoot, colStructs[i].fColPartition, colStructs[i].fColSegment);
        colOp->findTypeHandler(colStructs[i].colWidth, colStructs[i].colDataType);

        string segFile;
        rc = colOp->openColumnFile(curCol, segFile, true, IO_BUFF_SIZE);  // @bug 5572 HDFS tmp file

        if (rc != NO_ERROR)  // If openFile fails, disk error or header error is assumed.
        {
          // report error and return.
          std::ostringstream oss;
          WErrorCodes ec;
          string err = ec.errorString(rc);
          oss << "Error opening file oid:dbroot:partition:segment = " << colStructs[i].dataOid << ":"
              << colStructs[i].fColDbRoot << ":" << colStructs[i].fColPartition << ":"
              << colStructs[i].fColSegment << " and error code is " << rc << " with message " << err;
          throw std::runtime_error(oss.str());
        }

        allocateValArray(valArray, 1, colStructs[i].colType, colStructs[i].colWidth);

        rc = colOp->writeRows(curCol, ridList.size(), ridList, valArray, 0, true);

        if (rc != NO_ERROR)
        {
          // read error is fixed in place
          if (rc == ERR_COMP_COMPRESS)  // write error
          {
          }
        }

        // flush files will be done in the end of fix.
        colOp->clearColumn(curCol);

        if (valArray != NULL)
          free(valArray);
      }
      else  // dictionary file. How to fix
      {
        // read headers out, uncompress the last chunk, if error, replace it with empty chunk.
        Dctnry* dctnry = m_dctnry[op(dctnryStructList[i].fCompressionType)];
        rc = dctnry->openDctnry(dctnryStructList[i].dctnryOid, dctnryStructList[i].fColDbRoot,
                                dctnryStructList[i].fColPartition, dctnryStructList[i].fColSegment, false);

        rc = dctnry->checkFixLastDictChunk();
        rc = dctnry->closeDctnry(true);
      }
    }
  }

  return rc;
}

/*@flushVMCache - Flush VM cache
 */
/***********************************************************
 * DESCRIPTION:
 *    Flush sytem VM cache
 * PARAMETERS:
 *    none
 * RETURN:
 *    none
 ***********************************************************/
void WriteEngineWrapper::flushVMCache() const
{
  //      int fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
  //      write(fd, "3", 1);
  //      close(fd);
}

#if 0
// XXX temporary for debugging purposes
static void log_this(const char *message,
    logging::LOG_TYPE log_type, unsigned sid)
{
    // corresponds with dbcon in SubsystemID vector
    // in messagelog.cpp
    unsigned int subSystemId = 24;
    logging::LoggingID logid( subSystemId, sid, 0);
    logging::Message::Args args1;
    logging::Message msg(1);
    args1.add(message);
    msg.format( args1 );
    Logger logger(logid.fSubsysID);
    logger.logMessage(log_type, msg, logid);
}
#endif

/** @brief Determine whether we may update a column's ranges (by type) and return nullptr if we can't */
static ExtCPInfo* getCPInfoToUpdateForUpdatableType(const ColStruct& colStruct, ExtCPInfo* currentCPInfo)
{
  if (colStruct.tokenFlag)
  {
    return nullptr;
  }
  switch (colStruct.colType)
  {
    // here we enumerate all supported types.
    case WR_BYTE:
    case WR_SHORT:
    case WR_INT:
    case WR_LONGLONG:
    case WR_CHAR:
    case WR_UBYTE:
    case WR_USHORT:
    case WR_UINT:
    case WR_ULONGLONG:
    case WR_MEDINT:
    case WR_UMEDINT:
    case WR_BINARY:
    {
      return currentCPInfo;
    }
    // all unsupported types must not be supported.
    default: return nullptr;  // safe choice for everything we can't do.
  }
}

/** @brief Let only valid ranges to be present.
 *
 * There can be a case that we have computed invalid range while computing updated ranges.
 *
 * These invalid ranges should not have CP_VALID in the status code. So, they must not
 * come into final call to setCPInfos or something.
 *
 * To achieve that, we filter these invalid ranges here.
 */
static void setInvalidCPInfosSpecialMarks(std::vector<ExtCPInfo>& cpInfos)
{
  size_t i;
  for (i = 0; i < cpInfos.size(); i++)
  {
    if (cpInfos[i].isInvalid())
    {
      cpInfos[i].fCPInfo.seqNum = SEQNUM_MARK_INVALID_SET_RANGE;
    }
  }
}

/*@insertColumnRecs -  Insert value(s) into a column
 */
/***********************************************************
 * DESCRIPTION:
 *    Insert values into  columns (batchinsert)
 * PARAMETERS:
 *    colStructList - column struct list
 *    colValueList - column value list
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in inserting the value
 ***********************************************************/

int WriteEngineWrapper::insertColumnRecs(
    const TxnID& txnid, const CSCTypesList& cscColTypeList, ColStructList& colStructList,
    ColValueList& colValueList, DctnryStructList& dctnryStructList, DictStrList& dictStrList,
    std::vector<boost::shared_ptr<DBRootExtentTracker> >& dbRootExtentTrackers, RBMetaWriter* fRBMetaWriter,
    bool bFirstExtentOnThisPM, bool insertSelect, bool isAutoCommitOn, OID tableOid, bool isFirstBatchPm)
{
  int rc;
  RID* rowIdArray = NULL;
  ColTupleList curTupleList;
  Column curCol;
  ColStruct curColStruct;
  ColValueList colOldValueList;
  ColValueList colNewValueList;
  ColStructList newColStructList;
  DctnryStructList newDctnryStructList;
  HWM hwm = 0;
  HWM oldHwm = 0;
  HWM newHwm = 0;
  ColTupleList::size_type totalRow;
  ColStructList::size_type totalColumns;
  uint64_t rowsLeft = 0;
  bool newExtent = false;
  RIDList ridList;
  ColumnOp* colOp = NULL;
  ColSplitMaxMinInfoList maxMins;

  // Set tmp file suffix to modify HDFS db file
  bool useTmpSuffix = false;

  m_opType = INSERT;

  if (idbdatafile::IDBPolicy::useHdfs())
  {
    if (!bFirstExtentOnThisPM)
      useTmpSuffix = true;
  }

  unsigned i = 0;
#ifdef PROFILE
  StopWatch timer;
#endif

  // debug information for testing
  if (isDebug(DEBUG_2))
  {
    printInputValue(colStructList, colValueList, ridList, dctnryStructList, dictStrList);
  }

  // end

  // Convert data type and column width to write engine specific
  for (i = 0; i < colStructList.size(); i++)
    Convertor::convertColType(&colStructList[i]);

  for (const auto& colStruct : colStructList)
  {
    ColSplitMaxMinInfo tmp(colStruct.colDataType, colStruct.colWidth);
    maxMins.push_back(tmp);
  }

  uint32_t colId = 0;
  // MCOL-1675: find the smallest column width to calculate the RowID from so
  // that all HWMs will be incremented by this operation
  findSmallestColumn(colId, colStructList);

  // rc = checkValid(txnid, colStructList, colValueList, ridList);
  // if (rc != NO_ERROR)
  //   return rc;

  setTransId(txnid);
  uint16_t dbRoot, segmentNum;
  uint32_t partitionNum;
  string segFile;
  bool newFile;
  TableMetaData* tableMetaData = TableMetaData::makeTableMetaData(tableOid);
  // populate colStructList with file information
  IDBDataFile* pFile = NULL;
  std::vector<DBRootExtentInfo> extentInfo;
  int currentDBrootIdx = 0;
  std::vector<BRM::CreateStripeColumnExtentsArgOut>
      extents;  // this structure valid only when isFirstOnBatchPm is true.
  std::vector<BRM::LBID_t> newExtentsStartingLbids;  // we keep column-indexed LBIDs here for **new** extents.
                                                     // It is set and valid and used only when rowsLeft > 0.

  //--------------------------------------------------------------------------
  // For first batch on this PM:
  //   o get starting extent from ExtentTracker, and allocate extent if needed
  //   o construct colStructList and dctnryStructList accordingly
  //   o save extent information in tableMetaData for future use
  // If not first batch on this PM:
  //   o construct colStructList and dctnryStructList from tableMetaData
  //--------------------------------------------------------------------------
  if (isFirstBatchPm)
  {
    currentDBrootIdx = dbRootExtentTrackers[colId]->getCurrentDBRootIdx();
    extentInfo = dbRootExtentTrackers[colId]->getDBRootExtentList();
    dbRoot = extentInfo[currentDBrootIdx].fDbRoot;
    partitionNum = extentInfo[currentDBrootIdx].fPartition;

    //----------------------------------------------------------------------
    // check whether this extent is the first on this PM
    //----------------------------------------------------------------------
    if (bFirstExtentOnThisPM)
    {
      std::vector<BRM::CreateStripeColumnExtentsArgIn> cols;
      BRM::CreateStripeColumnExtentsArgIn createStripeColumnExtentsArgIn;

      for (i = 0; i < colStructList.size(); i++)
      {
        createStripeColumnExtentsArgIn.oid = colStructList[i].dataOid;
        createStripeColumnExtentsArgIn.width = colStructList[i].colWidth;
        createStripeColumnExtentsArgIn.colDataType = colStructList[i].colDataType;
        cols.push_back(createStripeColumnExtentsArgIn);
      }

      rc = BRMWrapper::getInstance()->allocateStripeColExtents(cols, dbRoot, partitionNum, segmentNum,
                                                               extents);

      if (rc != NO_ERROR)
        return rc;

      // Create column files
      ExtCPInfoList cpinfoList;

      for (i = 0; i < extents.size(); i++)
      {
        ExtCPInfo cpInfo(colStructList[i].colDataType, colStructList[i].colWidth);
        colOp = m_colOp[op(colStructList[i].fCompressionType)];
        colOp->initColumn(curCol);
        colOp->setColParam(curCol, colId, colStructList[i].colWidth, colStructList[i].colDataType,
                           colStructList[i].colType, colStructList[i].dataOid,
                           colStructList[i].fCompressionType, dbRoot, partitionNum, segmentNum);
        colOp->findTypeHandler(colStructList[i].colWidth, colStructList[i].colDataType);
        rc = colOp->extendColumn(curCol, false, extents[i].startBlkOffset, extents[i].startLbid,
                                 extents[i].allocSize, dbRoot, partitionNum, segmentNum, segFile, pFile,
                                 newFile);

        if (rc != NO_ERROR)
          return rc;

        cpInfo.toInvalid();

        cpInfo.fCPInfo.seqNum = SEQNUM_MARK_INVALID_SET_RANGE;

        // mark the extents to invalid
        cpInfo.fCPInfo.firstLbid = extents[i].startLbid;
        cpinfoList.push_back(cpInfo);
        colStructList[i].fColPartition = partitionNum;
        colStructList[i].fColSegment = segmentNum;
        colStructList[i].fColDbRoot = dbRoot;
        dctnryStructList[i].fColPartition = partitionNum;
        dctnryStructList[i].fColSegment = segmentNum;
        dctnryStructList[i].fColDbRoot = dbRoot;
      }

      // mark the extents to invalid
      rc = BRMWrapper::getInstance()->setExtentsMaxMin(cpinfoList);

      if (rc != NO_ERROR)
        return rc;

      // create corresponding dictionary files
      for (i = 0; i < dctnryStructList.size(); i++)
      {
        if (dctnryStructList[i].dctnryOid > 0)
        {
          rc = createDctnry(txnid, dctnryStructList[i].dctnryOid, dctnryStructList[i].colWidth, dbRoot,
                            partitionNum, segmentNum, dctnryStructList[i].fCompressionType);

          if (rc != NO_ERROR)
            return rc;
        }
      }
    }     // if ( bFirstExtentOnThisPM)
    else  // if (!bFirstExtentOnThisPM)
    {
      std::vector<DBRootExtentInfo> tmpExtentInfo;

      for (i = 0; i < dbRootExtentTrackers.size(); i++)
      {
        tmpExtentInfo = dbRootExtentTrackers[i]->getDBRootExtentList();
        colStructList[i].fColPartition = tmpExtentInfo[currentDBrootIdx].fPartition;
        colStructList[i].fColSegment = tmpExtentInfo[currentDBrootIdx].fSegment;
        colStructList[i].fColDbRoot = tmpExtentInfo[currentDBrootIdx].fDbRoot;
        dctnryStructList[i].fColPartition = tmpExtentInfo[currentDBrootIdx].fPartition;
        dctnryStructList[i].fColSegment = tmpExtentInfo[currentDBrootIdx].fSegment;
        dctnryStructList[i].fColDbRoot = tmpExtentInfo[currentDBrootIdx].fDbRoot;
      }
    }

    //----------------------------------------------------------------------
    // Save the extents info in tableMetaData
    //----------------------------------------------------------------------
    for (i = 0; i < colStructList.size(); i++)
    {
      ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[i].dataOid);
      ColExtsInfo::iterator it = aColExtsInfo.begin();

      while (it != aColExtsInfo.end())
      {
        if ((it->dbRoot == colStructList[i].fColDbRoot) && (it->partNum == colStructList[i].fColPartition) &&
            (it->segNum == colStructList[i].fColSegment))
          break;

        it++;
      }

      if (it == aColExtsInfo.end())  // add this one to the list
      {
        ColExtInfo aExt;
        aExt.dbRoot = colStructList[i].fColDbRoot;
        aExt.partNum = colStructList[i].fColPartition;
        aExt.segNum = colStructList[i].fColSegment;
        aExt.compType = colStructList[i].fCompressionType;
        aExt.isDict = false;

        if (bFirstExtentOnThisPM)
        {
          aExt.hwm = extents[i].startBlkOffset;
          aExt.isNewExt = true;
        }
        else
        {
          std::vector<DBRootExtentInfo> tmpExtentInfo;
          tmpExtentInfo = dbRootExtentTrackers[i]->getDBRootExtentList();
          aExt.isNewExt = false;
          aExt.hwm = tmpExtentInfo[currentDBrootIdx].fLocalHwm;
        }

        aExt.current = true;
        aColExtsInfo.push_back(aExt);
      }

      tableMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
    }

    for (i = 0; i < dctnryStructList.size(); i++)
    {
      if (dctnryStructList[i].dctnryOid > 0)
      {
        ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(dctnryStructList[i].dctnryOid);
        ColExtsInfo::iterator it = aColExtsInfo.begin();

        while (it != aColExtsInfo.end())
        {
          if ((it->dbRoot == dctnryStructList[i].fColDbRoot) &&
              (it->partNum == dctnryStructList[i].fColPartition) &&
              (it->segNum == dctnryStructList[i].fColSegment))
            break;

          it++;
        }

        if (it == aColExtsInfo.end())  // add this one to the list
        {
          ColExtInfo aExt;
          aExt.dbRoot = dctnryStructList[i].fColDbRoot;
          aExt.partNum = dctnryStructList[i].fColPartition;
          aExt.segNum = dctnryStructList[i].fColSegment;
          aExt.compType = dctnryStructList[i].fCompressionType;
          aExt.isDict = true;
          aColExtsInfo.push_back(aExt);
        }

        tableMetaData->setColExtsInfo(dctnryStructList[i].dctnryOid, aColExtsInfo);
      }
    }

  }     // if (isFirstBatchPm)
  else  // get the extent info from tableMetaData
  {
    ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[colId].dataOid);
    ColExtsInfo::iterator it = aColExtsInfo.begin();

    while (it != aColExtsInfo.end())
    {
      if (it->current)
        break;

      it++;
    }

    if (it == aColExtsInfo.end())
      return 1;

    for (i = 0; i < colStructList.size(); i++)
    {
      colStructList[i].fColPartition = it->partNum;
      colStructList[i].fColSegment = it->segNum;
      colStructList[i].fColDbRoot = it->dbRoot;
      dctnryStructList[i].fColPartition = it->partNum;
      dctnryStructList[i].fColSegment = it->segNum;
      dctnryStructList[i].fColDbRoot = it->dbRoot;
    }
  }

  curTupleList = static_cast<ColTupleList>(colValueList[0]);
  totalRow = curTupleList.size();
  totalColumns = colStructList.size();
  rowIdArray = new RID[totalRow];
  // use scoped_array to ensure ptr deletion regardless of where we return
  boost::scoped_array<RID> rowIdArrayPtr(rowIdArray);
  memset(rowIdArray, 0, (sizeof(RID) * totalRow));

  //--------------------------------------------------------------------------
  // allocate row id(s)
  //--------------------------------------------------------------------------
  curColStruct = colStructList[colId];
  colOp = m_colOp[op(curColStruct.fCompressionType)];

  colOp->initColumn(curCol);

  // Get the correct segment, partition, column file
  vector<ExtentInfo> colExtentInfo;   // Save those empty extents in case of failure to rollback
  vector<ExtentInfo> dictExtentInfo;  // Save those empty extents in case of failure to rollback
  vector<ExtentInfo> fileInfo;
  dbRoot = curColStruct.fColDbRoot;
  // use the first column to calculate row id
  ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[colId].dataOid);
  ColExtsInfo::iterator it = aColExtsInfo.begin();

  while (it != aColExtsInfo.end())
  {
    if ((it->dbRoot == colStructList[colId].fColDbRoot) &&
        (it->partNum == colStructList[colId].fColPartition) &&
        (it->segNum == colStructList[colId].fColSegment) && it->current)
    {
      break;
    }
    it++;
  }

  if (it != aColExtsInfo.end())
  {
    hwm = it->hwm;
  }

  oldHwm = hwm;  // Save this info for rollback
  // need to pass real dbRoot, partition, and segment to setColParam
  colOp->setColParam(curCol, colId, curColStruct.colWidth, curColStruct.colDataType, curColStruct.colType,
                     curColStruct.dataOid, curColStruct.fCompressionType, curColStruct.fColDbRoot,
                     curColStruct.fColPartition, curColStruct.fColSegment);
  colOp->findTypeHandler(curColStruct.colWidth, curColStruct.colDataType);
  rc = colOp->openColumnFile(curCol, segFile, useTmpSuffix);  // @bug 5572 HDFS tmp file

  if (rc != NO_ERROR)
  {
    return rc;
  }

  // get hwm first
  // @bug 286 : fix for bug 286 - correct the typo in getHWM
  // RETURN_ON_ERROR(BRMWrapper::getInstance()->getHWM(curColStruct.dataOid, hwm));

  Column newCol;

#ifdef PROFILE
  timer.start("allocRowId");
#endif
  newColStructList = colStructList;
  newDctnryStructList = dctnryStructList;
  bool bUseStartExtent = true;

  if (idbdatafile::IDBPolicy::useHdfs())
    insertSelect = true;

  rc = colOp->allocRowId(txnid, bUseStartExtent, curCol, (uint64_t)totalRow, rowIdArray, hwm, newExtent,
                         rowsLeft, newHwm, newFile, newColStructList, newDctnryStructList,
                         dbRootExtentTrackers, insertSelect, true, tableOid, isFirstBatchPm,
                         &newExtentsStartingLbids);

  if (rc != NO_ERROR)  // Clean up is already done
    return rc;

#ifdef PROFILE
  timer.stop("allocRowId");
#endif

  //--------------------------------------------------------------------------
  // Expand initial abbreviated extent if any RID in 1st extent is > 256K.
  // if totalRow == rowsLeft, then not adding rows to 1st extent, so skip it.
  //--------------------------------------------------------------------------
  // DMC-SHARED_NOTHING_NOTE: Is it safe to assume only part0 seg0 is abbreviated?
  if ((curCol.dataFile.fPartition == 0) && (curCol.dataFile.fSegment == 0) && ((totalRow - rowsLeft) > 0) &&
      (rowIdArray[totalRow - rowsLeft - 1] >= (RID)INITIAL_EXTENT_ROWS_TO_DISK))
  {
    for (unsigned k = 0; k < colStructList.size(); k++)
    {
      // Skip the selected column
      if (k == colId)
        continue;
      Column expandCol;
      colOp = m_colOp[op(colStructList[k].fCompressionType)];
      colOp->setColParam(expandCol, 0, colStructList[k].colWidth, colStructList[k].colDataType,
                         colStructList[k].colType, colStructList[k].dataOid,
                         colStructList[k].fCompressionType, colStructList[k].fColDbRoot,
                         colStructList[k].fColPartition, colStructList[k].fColSegment);
      colOp->findTypeHandler(colStructList[k].colWidth, colStructList[k].colDataType);
      rc = colOp->openColumnFile(expandCol, segFile, true);  // @bug 5572 HDFS tmp file

      if (rc == NO_ERROR)
      {
        if (colOp->abbreviatedExtent(expandCol.dataFile.pFile, colStructList[k].colWidth))
        {
          rc = colOp->expandAbbrevExtent(expandCol);
        }
      }

      if (rc != NO_ERROR)
      {
        return rc;
      }

      colOp->clearColumn(expandCol);  // closes the file (if uncompressed)
    }
  }

  //--------------------------------------------------------------------------
  // Tokenize data if needed
  //--------------------------------------------------------------------------
  if (insertSelect && isAutoCommitOn)
    BRMWrapper::setUseVb(false);
  else
    BRMWrapper::setUseVb(true);

  dictStr::iterator dctStr_iter;
  ColTupleList::iterator col_iter;

  for (i = 0; i < colStructList.size(); i++)
  {
    if (colStructList[i].tokenFlag)
    {
      dctStr_iter = dictStrList[i].begin();
      col_iter = colValueList[i].begin();
      Dctnry* dctnry = m_dctnry[op(dctnryStructList[i].fCompressionType)];
      rc = dctnry->openDctnry(dctnryStructList[i].dctnryOid, dctnryStructList[i].fColDbRoot,
                              dctnryStructList[i].fColPartition, dctnryStructList[i].fColSegment,
                              useTmpSuffix);  // @bug 5572 HDFS tmp file

      if (rc != NO_ERROR)
      {
        // cout << "Error opening dctnry file " << dctnryStructList[i].dctnryOid << endl;
        return rc;
      }

      for (uint32_t rows = 0; rows < (totalRow - rowsLeft); rows++)
      {
        if (dctStr_iter->length() == 0)
        {
          Token nullToken;
          col_iter->data = nullToken;
        }
        else
        {
#ifdef PROFILE
          timer.start("tokenize");
#endif
          DctnryTuple dctTuple;
          dctTuple.sigValue = (unsigned char*)dctStr_iter->c_str();
          dctTuple.sigSize = dctStr_iter->length();
          dctTuple.isNull = false;
          rc = tokenize(txnid, dctTuple, dctnryStructList[i].fCompressionType);

          if (rc != NO_ERROR)
          {
            dctnry->closeDctnry();
            return rc;
          }

#ifdef PROFILE
          timer.stop("tokenize");
#endif
          col_iter->data = dctTuple.token;
        }

        dctStr_iter++;
        col_iter++;
      }

      // close dictionary files
      rc = dctnry->closeDctnry(false);

      if (rc != NO_ERROR)
        return rc;

      if (newExtent)
      {
        //@Bug 4854 back up hwm chunk for the file to be modified
        if (fRBMetaWriter)
          fRBMetaWriter->backupDctnryHWMChunk(
              newDctnryStructList[i].dctnryOid, newDctnryStructList[i].fColDbRoot,
              newDctnryStructList[i].fColPartition, newDctnryStructList[i].fColSegment);

        rc = dctnry->openDctnry(newDctnryStructList[i].dctnryOid, newDctnryStructList[i].fColDbRoot,
                                newDctnryStructList[i].fColPartition, newDctnryStructList[i].fColSegment,
                                false);  // @bug 5572 HDFS tmp file

        if (rc != NO_ERROR)
          return rc;

        for (uint32_t rows = 0; rows < rowsLeft; rows++)
        {
          if (dctStr_iter->length() == 0)
          {
            Token nullToken;
            col_iter->data = nullToken;
          }
          else
          {
#ifdef PROFILE
            timer.start("tokenize");
#endif
            DctnryTuple dctTuple;
            dctTuple.sigValue = (unsigned char*)dctStr_iter->c_str();
            dctTuple.sigSize = dctStr_iter->length();
            dctTuple.isNull = false;
            rc = tokenize(txnid, dctTuple, newDctnryStructList[i].fCompressionType);

            if (rc != NO_ERROR)
            {
              dctnry->closeDctnry();
              return rc;
            }

#ifdef PROFILE
            timer.stop("tokenize");
#endif
            col_iter->data = dctTuple.token;
          }

          dctStr_iter++;
          col_iter++;
        }

        // close dictionary files
        rc = dctnry->closeDctnry(false);

        if (rc != NO_ERROR)
          return rc;
      }
    }
  }

  if (insertSelect && isAutoCommitOn)
    BRMWrapper::setUseVb(false);
  else
    BRMWrapper::setUseVb(true);

  //--------------------------------------------------------------------------
  // Update column info structure @Bug 1862 set hwm, and
  // Prepare ValueList for new extent (if applicable)
  //--------------------------------------------------------------------------
  //@Bug 2205 Check whether all rows go to the new extent
  RID lastRid = 0;
  RID lastRidNew = 0;

  if (totalRow - rowsLeft > 0)
  {
    lastRid = rowIdArray[totalRow - rowsLeft - 1];
    lastRidNew = rowIdArray[totalRow - 1];
  }
  else
  {
    lastRid = 0;
    lastRidNew = rowIdArray[totalRow - 1];
  }

  // if a new extent is created, all the columns in this table should have their own new extent
  // First column already processed

  //@Bug 1701. Close the file (if uncompressed)
  m_colOp[op(curCol.compressionType)]->clearColumn(curCol);
  // Update hwm to set them in the end
  bool succFlag = false;
  unsigned colWidth = 0;
  int curFbo = 0, curBio;

  for (i = 0; i < totalColumns; i++)
  {
    // shoud be obtained from saved hwm
    aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[i].dataOid);
    it = aColExtsInfo.begin();

    while (it != aColExtsInfo.end())
    {
      if ((it->dbRoot == colStructList[i].fColDbRoot) && (it->partNum == colStructList[i].fColPartition) &&
          (it->segNum == colStructList[i].fColSegment) && it->current)
        break;

      it++;
    }

    if (it != aColExtsInfo.end())  // update hwm info
    {
      oldHwm = it->hwm;
    }

    // save hwm for the old extent
    colWidth = colStructList[i].colWidth;
    succFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK / colWidth, colWidth, curFbo, curBio);

    if (succFlag)
    {
      if ((HWM)curFbo >= oldHwm)
      {
        it->hwm = (HWM)curFbo;
      }

      //@Bug 4947. set current to false for old extent.
      if (newExtent)
      {
        it->current = false;
      }
    }
    else
      return ERR_INVALID_PARAM;

    // update hwm for the new extent
    if (newExtent)
    {
      it = aColExtsInfo.begin();

      while (it != aColExtsInfo.end())
      {
        if ((it->dbRoot == newColStructList[i].fColDbRoot) &&
            (it->partNum == newColStructList[i].fColPartition) &&
            (it->segNum == newColStructList[i].fColSegment) && it->current)
          break;

        it++;
      }

      succFlag = colOp->calculateRowId(lastRidNew, BYTE_PER_BLOCK / colWidth, colWidth, curFbo, curBio);

      if (succFlag)
      {
        if (it != aColExtsInfo.end())
        {
          it->hwm = (HWM)curFbo;
          // cout << "setting hwm to " << (int)curFbo <<" for seg " <<it->segNum << endl;
          it->current = true;
        }
      }
      else
        return ERR_INVALID_PARAM;
    }

    tableMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
  }

  //--------------------------------------------------------------------------
  // Prepare the valuelist for the new extent
  //--------------------------------------------------------------------------
  ColTupleList colTupleList;
  ColTupleList newColTupleList;
  ColTupleList firstPartTupleList;

  for (unsigned i = 0; i < totalColumns; i++)
  {
    colTupleList = static_cast<ColTupleList>(colValueList[i]);

    for (uint64_t j = rowsLeft; j > 0; j--)
    {
      newColTupleList.push_back(colTupleList[totalRow - j]);
    }

    colNewValueList.push_back(newColTupleList);
    newColTupleList.clear();

    // update the oldvalue list for the old extent
    for (uint64_t j = 0; j < (totalRow - rowsLeft); j++)
    {
      firstPartTupleList.push_back(colTupleList[j]);
    }

    colOldValueList.push_back(firstPartTupleList);
    firstPartTupleList.clear();
  }

  // end of allocate row id

#ifdef PROFILE
  timer.start("writeColumnRec");
#endif

  if (rc == NO_ERROR)
  {
    //----------------------------------------------------------------------
    // Mark extents invalid
    //----------------------------------------------------------------------
    bool successFlag = true;
    unsigned width = 0;
    int curFbo = 0, curBio, lastFbo = -1;

    if (isFirstBatchPm && (totalRow == rowsLeft))
    {
      // in this particular case we already marked extents as invalid up there.
    }
    else
    {
      int firstHalfCount = totalRow - rowsLeft;
      for (unsigned i = 0; i < colStructList.size(); i++)
      {
        colOp = m_colOp[op(colStructList[i].fCompressionType)];
        width = colStructList[i].colWidth;
        if (firstHalfCount)
        {
          ExtCPInfo* cpInfoP =
              getCPInfoToUpdateForUpdatableType(colStructList[i], &maxMins[i].fSplitMaxMinInfo[0]);
          RID thisRid = rowsLeft ? lastRid : lastRidNew;
          successFlag = colOp->calculateRowId(thisRid, BYTE_PER_BLOCK / width, width, curFbo, curBio);

          if (successFlag)
          {
            if (curFbo != lastFbo)
            {
              RETURN_ON_ERROR(AddLBIDtoList(txnid, colStructList[i], curFbo, cpInfoP));
            }
          }
          maxMins[i].fSplitMaxMinInfoPtrs[0] = cpInfoP;
        }
        if (rowsLeft)
        {
          ExtCPInfo* cpInfoP =
              getCPInfoToUpdateForUpdatableType(colStructList[i], &maxMins[i].fSplitMaxMinInfo[1]);
          if (cpInfoP)
          {
            RETURN_ON_ERROR(GetLBIDRange(newExtentsStartingLbids[i], colStructList[i], *cpInfoP));
          }

          maxMins[i].fSplitMaxMinInfoPtrs[1] = cpInfoP;
        }
      }
    }

    markTxnExtentsAsInvalid(txnid);

    //----------------------------------------------------------------------
    // Write row(s) to database file(s)
    //----------------------------------------------------------------------
    std::vector<ExtCPInfo> cpinfoList;
    for (auto& splitCPInfo : maxMins)
    {
      for (i = 0; i < 2; i++)
      {
        ExtCPInfo* cpInfo = splitCPInfo.fSplitMaxMinInfoPtrs[i];
        if (cpInfo)
        {
          cpinfoList.push_back(*cpInfo);
          cpinfoList[cpinfoList.size() - 1].fCPInfo.seqNum = SEQNUM_MARK_INVALID_SET_RANGE;
        }
      }
    }
    rc = BRMWrapper::getInstance()->setExtentsMaxMin(cpinfoList);
    if (rc != NO_ERROR)
    {
      return rc;
    }
    rc = writeColumnRec(txnid, cscColTypeList, colStructList, colOldValueList, rowIdArray, newColStructList,
                        colNewValueList, tableOid, useTmpSuffix, true, &maxMins);  // @bug 5572 HDFS tmp file

    if (rc == NO_ERROR)
    {
      if (dctnryStructList.size() > 0)
      {
        vector<BRM::OID_t> oids{static_cast<int32_t>(tableOid)};
        for (const DctnryStruct& dctnryStruct : dctnryStructList)
        {
          oids.push_back(dctnryStruct.dctnryOid);
        }

        rc = flushOIDsFromCache(oids);

        if (rc != 0)
          rc = ERR_BLKCACHE_FLUSH_LIST;  // translate to WE error
      }

      if (rc == NO_ERROR)
      {
        int index = 0;
        for (auto& splitCPInfo : maxMins)
        {
          for (i = 0; i < 2; i++)
          {
            ExtCPInfo* cpInfo = splitCPInfo.fSplitMaxMinInfoPtrs[i];
            if (cpInfo)
            {
              cpinfoList[index] = *cpInfo;
              cpinfoList[index].fCPInfo.seqNum++;
              index++;
            }
          }
        }
        setInvalidCPInfosSpecialMarks(cpinfoList);
        rc = BRMWrapper::getInstance()->setExtentsMaxMin(cpinfoList);
      }
    }
  }

  return rc;
}

int WriteEngineWrapper::insertColumnRecsBinary(
    const TxnID& txnid, ColStructList& colStructList, std::vector<uint64_t>& colValueList,
    DctnryStructList& dctnryStructList, DictStrList& dictStrList,
    std::vector<boost::shared_ptr<DBRootExtentTracker> >& dbRootExtentTrackers, RBMetaWriter* fRBMetaWriter,
    bool bFirstExtentOnThisPM, bool insertSelect, bool isAutoCommitOn, OID tableOid, bool isFirstBatchPm)
{
  int rc;
  RID* rowIdArray = NULL;
  Column curCol;
  ColStruct curColStruct;
  ColStructList newColStructList;
  std::vector<uint64_t> colNewValueList;
  DctnryStructList newDctnryStructList;
  HWM hwm = 0;
  HWM oldHwm = 0;
  HWM newHwm = 0;
  size_t totalRow;
  ColStructList::size_type totalColumns;
  uint64_t rowsLeft = 0;
  bool newExtent = false;
  RIDList ridList;
  ColumnOp* colOp = NULL;
  std::vector<BRM::LBID_t> dictLbids;

  // Set tmp file suffix to modify HDFS db file
  bool useTmpSuffix = false;

  m_opType = INSERT;

  if (idbdatafile::IDBPolicy::useHdfs())
  {
    if (!bFirstExtentOnThisPM)
      useTmpSuffix = true;
  }

  unsigned i = 0;
#ifdef PROFILE
  StopWatch timer;
#endif

  // Convert data type and column width to write engine specific
  for (i = 0; i < colStructList.size(); i++)
    Convertor::convertColType(&colStructList[i]);

  uint32_t colId = 0;
  // MCOL-1675: find the smallest column width to calculate the RowID from so
  // that all HWMs will be incremented by this operation
  findSmallestColumn(colId, colStructList);

  // rc = checkValid(txnid, colStructList, colValueList, ridList);
  // if (rc != NO_ERROR)
  //   return rc;

  setTransId(txnid);
  uint16_t dbRoot, segmentNum;
  uint32_t partitionNum;
  string segFile;
  bool newFile;
  TableMetaData* tableMetaData = TableMetaData::makeTableMetaData(tableOid);
  // populate colStructList with file information
  IDBDataFile* pFile = NULL;
  std::vector<DBRootExtentInfo> extentInfo;
  int currentDBrootIdx = 0;
  std::vector<BRM::CreateStripeColumnExtentsArgOut> extents;

  //--------------------------------------------------------------------------
  // For first batch on this PM:
  //   o get starting extent from ExtentTracker, and allocate extent if needed
  //   o construct colStructList and dctnryStructList accordingly
  //   o save extent information in tableMetaData for future use
  // If not first batch on this PM:
  //   o construct colStructList and dctnryStructList from tableMetaData
  //--------------------------------------------------------------------------
  if (isFirstBatchPm)
  {
    currentDBrootIdx = dbRootExtentTrackers[colId]->getCurrentDBRootIdx();
    extentInfo = dbRootExtentTrackers[colId]->getDBRootExtentList();
    dbRoot = extentInfo[currentDBrootIdx].fDbRoot;
    partitionNum = extentInfo[currentDBrootIdx].fPartition;

    //----------------------------------------------------------------------
    // check whether this extent is the first on this PM
    //----------------------------------------------------------------------
    if (bFirstExtentOnThisPM)
    {
      // cout << "bFirstExtentOnThisPM is " << bFirstExtentOnThisPM << endl;
      std::vector<BRM::CreateStripeColumnExtentsArgIn> cols;
      BRM::CreateStripeColumnExtentsArgIn createStripeColumnExtentsArgIn;

      for (i = 0; i < colStructList.size(); i++)
      {
        createStripeColumnExtentsArgIn.oid = colStructList[i].dataOid;
        createStripeColumnExtentsArgIn.width = colStructList[i].colWidth;
        createStripeColumnExtentsArgIn.colDataType = colStructList[i].colDataType;
        cols.push_back(createStripeColumnExtentsArgIn);
      }

      rc = BRMWrapper::getInstance()->allocateStripeColExtents(cols, dbRoot, partitionNum, segmentNum,
                                                               extents);

      if (rc != NO_ERROR)
        return rc;

      // Create column files
      ExtCPInfoList cpinfoList;

      for (i = 0; i < extents.size(); i++)
      {
        ExtCPInfo cpInfo(colStructList[i].colDataType, colStructList[i].colWidth);
        colOp = m_colOp[op(colStructList[i].fCompressionType)];
        colOp->initColumn(curCol);
        colOp->setColParam(curCol, 0, colStructList[i].colWidth, colStructList[i].colDataType,
                           colStructList[i].colType, colStructList[i].dataOid,
                           colStructList[i].fCompressionType, dbRoot, partitionNum, segmentNum);
        colOp->findTypeHandler(colStructList[i].colWidth, colStructList[i].colDataType);
        rc = colOp->extendColumn(curCol, false, extents[i].startBlkOffset, extents[i].startLbid,
                                 extents[i].allocSize, dbRoot, partitionNum, segmentNum, segFile, pFile,
                                 newFile);

        if (rc != NO_ERROR)
          return rc;

        cpInfo.toInvalid();

        cpInfo.fCPInfo.seqNum = SEQNUM_MARK_INVALID_SET_RANGE;

        // mark the extents to invalid
        cpInfo.fCPInfo.firstLbid = extents[i].startLbid;
        cpinfoList.push_back(cpInfo);
        colStructList[i].fColPartition = partitionNum;
        colStructList[i].fColSegment = segmentNum;
        colStructList[i].fColDbRoot = dbRoot;
        dctnryStructList[i].fColPartition = partitionNum;
        dctnryStructList[i].fColSegment = segmentNum;
        dctnryStructList[i].fColDbRoot = dbRoot;
      }

      // mark the extents to invalid
      rc = BRMWrapper::getInstance()->setExtentsMaxMin(cpinfoList);

      if (rc != NO_ERROR)
        return rc;

      // create corresponding dictionary files
      for (i = 0; i < dctnryStructList.size(); i++)
      {
        if (dctnryStructList[i].dctnryOid > 0)
        {
          rc = createDctnry(txnid, dctnryStructList[i].dctnryOid, dctnryStructList[i].colWidth, dbRoot,
                            partitionNum, segmentNum, dctnryStructList[i].fCompressionType);

          if (rc != NO_ERROR)
            return rc;
        }
      }
    }     // if ( bFirstExtentOnThisPM)
    else  // if (!bFirstExtentOnThisPM)
    {
      std::vector<DBRootExtentInfo> tmpExtentInfo;

      for (i = 0; i < dbRootExtentTrackers.size(); i++)
      {
        tmpExtentInfo = dbRootExtentTrackers[i]->getDBRootExtentList();
        colStructList[i].fColPartition = tmpExtentInfo[currentDBrootIdx].fPartition;
        colStructList[i].fColSegment = tmpExtentInfo[currentDBrootIdx].fSegment;
        colStructList[i].fColDbRoot = tmpExtentInfo[currentDBrootIdx].fDbRoot;
        // cout << "Load from dbrootExtenttracker oid:dbroot:part:seg = " <<colStructList[i].dataOid<<":"
        //<<colStructList[i].fColDbRoot<<":"<<colStructList[i].fColPartition<<":"<<colStructList[i].fColSegment<<endl;
        dctnryStructList[i].fColPartition = tmpExtentInfo[currentDBrootIdx].fPartition;
        dctnryStructList[i].fColSegment = tmpExtentInfo[currentDBrootIdx].fSegment;
        dctnryStructList[i].fColDbRoot = tmpExtentInfo[currentDBrootIdx].fDbRoot;
      }
    }

    //----------------------------------------------------------------------
    // Save the extents info in tableMetaData
    //----------------------------------------------------------------------
    for (i = 0; i < colStructList.size(); i++)
    {
      ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[i].dataOid);
      ColExtsInfo::iterator it = aColExtsInfo.begin();

      while (it != aColExtsInfo.end())
      {
        if ((it->dbRoot == colStructList[i].fColDbRoot) && (it->partNum == colStructList[i].fColPartition) &&
            (it->segNum == colStructList[i].fColSegment))
          break;

        it++;
      }

      if (it == aColExtsInfo.end())  // add this one to the list
      {
        ColExtInfo aExt;
        aExt.dbRoot = colStructList[i].fColDbRoot;
        aExt.partNum = colStructList[i].fColPartition;
        aExt.segNum = colStructList[i].fColSegment;
        aExt.compType = colStructList[i].fCompressionType;
        aExt.isDict = false;

        if (bFirstExtentOnThisPM)
        {
          aExt.hwm = extents[i].startBlkOffset;
          aExt.isNewExt = true;
          // cout << "adding a ext to metadata" << endl;
        }
        else
        {
          std::vector<DBRootExtentInfo> tmpExtentInfo;
          tmpExtentInfo = dbRootExtentTrackers[i]->getDBRootExtentList();
          aExt.isNewExt = false;
          aExt.hwm = tmpExtentInfo[currentDBrootIdx].fLocalHwm;
          // cout << "oid " << colStructList[i].dataOid << " gets hwm " << aExt.hwm << endl;
        }

        aExt.current = true;
        aColExtsInfo.push_back(aExt);
        // cout << "get from extentinfo oid:hwm = " << colStructList[i].dataOid << ":" << aExt.hwm << endl;
      }

      tableMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
    }

    for (i = 0; i < dctnryStructList.size(); i++)
    {
      if (dctnryStructList[i].dctnryOid > 0)
      {
        ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(dctnryStructList[i].dctnryOid);
        ColExtsInfo::iterator it = aColExtsInfo.begin();

        while (it != aColExtsInfo.end())
        {
          if ((it->dbRoot == dctnryStructList[i].fColDbRoot) &&
              (it->partNum == dctnryStructList[i].fColPartition) &&
              (it->segNum == dctnryStructList[i].fColSegment))
            break;

          it++;
        }

        if (it == aColExtsInfo.end())  // add this one to the list
        {
          ColExtInfo aExt;
          aExt.dbRoot = dctnryStructList[i].fColDbRoot;
          aExt.partNum = dctnryStructList[i].fColPartition;
          aExt.segNum = dctnryStructList[i].fColSegment;
          aExt.compType = dctnryStructList[i].fCompressionType;
          aExt.isDict = true;
          aColExtsInfo.push_back(aExt);
        }

        tableMetaData->setColExtsInfo(dctnryStructList[i].dctnryOid, aColExtsInfo);
      }
    }

  }     // if (isFirstBatchPm)
  else  // get the extent info from tableMetaData
  {
    ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[colId].dataOid);
    ColExtsInfo::iterator it = aColExtsInfo.begin();

    while (it != aColExtsInfo.end())
    {
      if (it->current)
        break;

      it++;
    }

    if (it == aColExtsInfo.end())
      return 1;

    for (i = 0; i < colStructList.size(); i++)
    {
      colStructList[i].fColPartition = it->partNum;
      colStructList[i].fColSegment = it->segNum;
      colStructList[i].fColDbRoot = it->dbRoot;
      dctnryStructList[i].fColPartition = it->partNum;
      dctnryStructList[i].fColSegment = it->segNum;
      dctnryStructList[i].fColDbRoot = it->dbRoot;
    }
  }

  totalColumns = colStructList.size();
  totalRow = colValueList.size() / totalColumns;
  rowIdArray = new RID[totalRow];
  // use scoped_array to ensure ptr deletion regardless of where we return
  boost::scoped_array<RID> rowIdArrayPtr(rowIdArray);
  memset(rowIdArray, 0, (sizeof(RID) * totalRow));

  //--------------------------------------------------------------------------
  // allocate row id(s)
  //--------------------------------------------------------------------------

  curColStruct = colStructList[colId];

  colOp = m_colOp[op(curColStruct.fCompressionType)];

  colOp->initColumn(curCol);

  // Get the correct segment, partition, column file
  vector<ExtentInfo> colExtentInfo;   // Save those empty extents in case of failure to rollback
  vector<ExtentInfo> dictExtentInfo;  // Save those empty extents in case of failure to rollback
  vector<ExtentInfo> fileInfo;
  dbRoot = curColStruct.fColDbRoot;
  // use the smallest column to calculate row id
  ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[colId].dataOid);
  ColExtsInfo::iterator it = aColExtsInfo.begin();

  while (it != aColExtsInfo.end())
  {
    if ((it->dbRoot == colStructList[colId].fColDbRoot) &&
        (it->partNum == colStructList[colId].fColPartition) &&
        (it->segNum == colStructList[colId].fColSegment) && it->current)
      break;

    it++;
  }

  if (it != aColExtsInfo.end())
  {
    hwm = it->hwm;
    // cout << "Got from colextinfo hwm for oid " << colStructList[colId].dataOid << " is " << hwm << " and
    // seg is " << colStructList[colId].fColSegment << endl;
  }

  oldHwm = hwm;  // Save this info for rollback
  // need to pass real dbRoot, partition, and segment to setColParam
  colOp->setColParam(curCol, colId, curColStruct.colWidth, curColStruct.colDataType, curColStruct.colType,
                     curColStruct.dataOid, curColStruct.fCompressionType, curColStruct.fColDbRoot,
                     curColStruct.fColPartition, curColStruct.fColSegment);
  colOp->findTypeHandler(curColStruct.colWidth, curColStruct.colDataType);
  rc = colOp->openColumnFile(curCol, segFile, useTmpSuffix);  // @bug 5572 HDFS tmp file

  if (rc != NO_ERROR)
  {
    return rc;
  }

  // get hwm first
  // @bug 286 : fix for bug 286 - correct the typo in getHWM
  // RETURN_ON_ERROR(BRMWrapper::getInstance()->getHWM(curColStruct.dataOid, hwm));

  Column newCol;

#ifdef PROFILE
  timer.start("allocRowId");
#endif
  newColStructList = colStructList;
  newDctnryStructList = dctnryStructList;
  bool bUseStartExtent = true;

  if (idbdatafile::IDBPolicy::useHdfs())
    insertSelect = true;

  rc = colOp->allocRowId(txnid, bUseStartExtent, curCol, (uint64_t)totalRow, rowIdArray, hwm, newExtent,
                         rowsLeft, newHwm, newFile, newColStructList, newDctnryStructList,
                         dbRootExtentTrackers, insertSelect, true, tableOid, isFirstBatchPm, NULL);

  // cout << "after allocrowid, total row = " <<totalRow << " newExtent is " << newExtent << endl;
  // cout << "column oid " << curColStruct.dataOid << " has hwm:newHwm = " << hwm <<":" << newHwm<< endl;
  if (rc != NO_ERROR)  // Clean up is already done
    return rc;

#ifdef PROFILE
  timer.stop("allocRowId");
#endif

  //--------------------------------------------------------------------------
  // Expand initial abbreviated extent if any RID in 1st extent is > 256K.
  // if totalRow == rowsLeft, then not adding rows to 1st extent, so skip it.
  //--------------------------------------------------------------------------
  // DMC-SHARED_NOTHING_NOTE: Is it safe to assume only part0 seg0 is abbreviated?
  if ((curCol.dataFile.fPartition == 0) && (curCol.dataFile.fSegment == 0) && ((totalRow - rowsLeft) > 0) &&
      (rowIdArray[totalRow - rowsLeft - 1] >= (RID)INITIAL_EXTENT_ROWS_TO_DISK))
  {
    for (size_t k = 0; k < colStructList.size(); k++)
    {
      // Skip the selected column
      if (k == (size_t)colId)
        continue;

      Column expandCol;
      colOp = m_colOp[op(colStructList[k].fCompressionType)];
      // Shouldn't we change 0 to colId here?
      colOp->setColParam(expandCol, 0, colStructList[k].colWidth, colStructList[k].colDataType,
                         colStructList[k].colType, colStructList[k].dataOid,
                         colStructList[k].fCompressionType, colStructList[k].fColDbRoot,
                         colStructList[k].fColPartition, colStructList[k].fColSegment);
      colOp->findTypeHandler(colStructList[k].colWidth, colStructList[k].colDataType);
      rc = colOp->openColumnFile(expandCol, segFile, true);  // @bug 5572 HDFS tmp file

      if (rc == NO_ERROR)
      {
        if (colOp->abbreviatedExtent(expandCol.dataFile.pFile, colStructList[k].colWidth))
        {
          rc = colOp->expandAbbrevExtent(expandCol);
        }
      }

      if (rc != NO_ERROR)
      {
        return rc;
      }

      colOp->closeColumnFile(expandCol);
    }
  }

  //--------------------------------------------------------------------------
  // Tokenize data if needed
  //--------------------------------------------------------------------------
  if (insertSelect && isAutoCommitOn)
    BRMWrapper::setUseVb(false);
  else
    BRMWrapper::setUseVb(true);

  dictStr::iterator dctStr_iter;
  uint64_t* colValPtr;
  size_t rowsPerColumn = colValueList.size() / colStructList.size();

  for (i = 0; i < colStructList.size(); i++)
  {
    if (colStructList[i].tokenFlag)
    {
      dctStr_iter = dictStrList[i].begin();
      Dctnry* dctnry = m_dctnry[op(dctnryStructList[i].fCompressionType)];
      rc = dctnry->openDctnry(dctnryStructList[i].dctnryOid, dctnryStructList[i].fColDbRoot,
                              dctnryStructList[i].fColPartition, dctnryStructList[i].fColSegment,
                              useTmpSuffix);  // @bug 5572 HDFS tmp file

      if (rc != NO_ERROR)
      {
        return rc;
      }

      for (uint32_t rows = 0; rows < (totalRow - rowsLeft); rows++)
      {
        colValPtr = &colValueList[(i * rowsPerColumn) + rows];

        if (dctStr_iter->length() == 0)
        {
          Token nullToken;
          memcpy(colValPtr, &nullToken, 8);
        }
        else
        {
#ifdef PROFILE
          timer.start("tokenize");
#endif
          DctnryTuple dctTuple;
          dctTuple.sigValue = (unsigned char*)dctStr_iter->c_str();
          dctTuple.sigSize = dctStr_iter->length();
          dctTuple.isNull = false;
          rc = tokenize(txnid, dctTuple, dctnryStructList[i].fCompressionType);

          if (rc != NO_ERROR)
          {
            dctnry->closeDctnry();
            return rc;
          }

#ifdef PROFILE
          timer.stop("tokenize");
#endif
          memcpy(colValPtr, &dctTuple.token, 8);
          dictLbids.push_back(dctTuple.token.fbo);
        }

        dctStr_iter++;
      }

      // close dictionary files
      rc = dctnry->closeDctnry(false);

      if (rc != NO_ERROR)
        return rc;

      if (newExtent)
      {
        //@Bug 4854 back up hwm chunk for the file to be modified
        if (fRBMetaWriter)
          fRBMetaWriter->backupDctnryHWMChunk(
              newDctnryStructList[i].dctnryOid, newDctnryStructList[i].fColDbRoot,
              newDctnryStructList[i].fColPartition, newDctnryStructList[i].fColSegment);

        rc = dctnry->openDctnry(newDctnryStructList[i].dctnryOid, newDctnryStructList[i].fColDbRoot,
                                newDctnryStructList[i].fColPartition, newDctnryStructList[i].fColSegment,
                                false);  // @bug 5572 HDFS tmp file

        if (rc != NO_ERROR)
          return rc;

        for (uint32_t rows = 0; rows < rowsLeft; rows++)
        {
          colValPtr = &colValueList[(i * rowsPerColumn) + rows];

          if (dctStr_iter->length() == 0)
          {
            Token nullToken;
            memcpy(colValPtr, &nullToken, 8);
          }
          else
          {
#ifdef PROFILE
            timer.start("tokenize");
#endif
            DctnryTuple dctTuple;
            dctTuple.sigValue = (unsigned char*)dctStr_iter->c_str();
            dctTuple.sigSize = dctStr_iter->length();
            dctTuple.isNull = false;
            rc = tokenize(txnid, dctTuple, newDctnryStructList[i].fCompressionType);

            if (rc != NO_ERROR)
            {
              dctnry->closeDctnry();
              return rc;
            }

#ifdef PROFILE
            timer.stop("tokenize");
#endif
            memcpy(colValPtr, &dctTuple.token, 8);
            dictLbids.push_back(dctTuple.token.fbo);
          }

          dctStr_iter++;
        }

        // close dictionary files
        rc = dctnry->closeDctnry(false);

        if (rc != NO_ERROR)
          return rc;
      }
    }
  }

  if (insertSelect && isAutoCommitOn)
    BRMWrapper::setUseVb(false);
  else
    BRMWrapper::setUseVb(true);

  //--------------------------------------------------------------------------
  // Update column info structure @Bug 1862 set hwm, and
  // Prepare ValueList for new extent (if applicable)
  //--------------------------------------------------------------------------
  //@Bug 2205 Check whether all rows go to the new extent
  RID lastRid = 0;
  RID lastRidNew = 0;

  if (totalRow - rowsLeft > 0)
  {
    lastRid = rowIdArray[totalRow - rowsLeft - 1];
    lastRidNew = rowIdArray[totalRow - 1];
  }
  else
  {
    lastRid = 0;
    lastRidNew = rowIdArray[totalRow - 1];
  }

  // cout << "rowid allocated is "  << lastRid << endl;
  // if a new extent is created, all the columns in this table should have their own new extent
  // First column already processed

  //@Bug 1701. Close the file (if uncompressed)
  m_colOp[op(curCol.compressionType)]->closeColumnFile(curCol);
  // cout << "Saving hwm info for new ext batch" << endl;
  // Update hwm to set them in the end
  bool succFlag = false;
  unsigned colWidth = 0;
  int curFbo = 0, curBio;

  for (i = 0; i < totalColumns; i++)
  {
    // shoud be obtained from saved hwm
    aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[i].dataOid);
    it = aColExtsInfo.begin();

    while (it != aColExtsInfo.end())
    {
      if ((it->dbRoot == colStructList[i].fColDbRoot) && (it->partNum == colStructList[i].fColPartition) &&
          (it->segNum == colStructList[i].fColSegment) && it->current)
        break;

      it++;
    }

    if (it != aColExtsInfo.end())  // update hwm info
    {
      oldHwm = it->hwm;

      // save hwm for the old extent
      colWidth = colStructList[i].colWidth;
      succFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK / colWidth, colWidth, curFbo, curBio);

      // cout << "insertcolumnrec   oid:rid:fbo:oldhwm = " << colStructList[i].dataOid << ":" << lastRid <<
      // ":" << curFbo << ":" << oldHwm << endl;
      if (succFlag)
      {
        if ((HWM)curFbo >= oldHwm)
        {
          it->hwm = (HWM)curFbo;
        }

        //@Bug 4947. set current to false for old extent.
        if (newExtent)
        {
          it->current = false;
        }

        // cout << "updated old ext info for oid " << colStructList[i].dataOid << "
        // dbroot:part:seg:hwm:current = "
        //<< it->dbRoot<<":"<<it->partNum<<":"<<it->segNum<<":"<<it->hwm<<":"<< it->current<< " and newExtent
        //is " << newExtent << endl;
      }
      else
        return ERR_INVALID_PARAM;
    }

    // update hwm for the new extent
    if (newExtent)
    {
      it = aColExtsInfo.begin();

      while (it != aColExtsInfo.end())
      {
        if ((it->dbRoot == newColStructList[i].fColDbRoot) &&
            (it->partNum == newColStructList[i].fColPartition) &&
            (it->segNum == newColStructList[i].fColSegment) && it->current)
          break;

        it++;
      }

      colWidth = newColStructList[i].colWidth;
      succFlag = colOp->calculateRowId(lastRidNew, BYTE_PER_BLOCK / colWidth, colWidth, curFbo, curBio);

      if (succFlag)
      {
        if (it != aColExtsInfo.end())
        {
          it->hwm = (HWM)curFbo;
          // cout << "setting hwm to " << (int)curFbo <<" for seg " <<it->segNum << endl;
          it->current = true;
        }
      }
      else
        return ERR_INVALID_PARAM;
    }

    tableMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
  }

  //--------------------------------------------------------------------------
  // Prepare the valuelist for the new extent
  //--------------------------------------------------------------------------

  for (unsigned i = 1; i <= totalColumns; i++)
  {
    // Copy values to second value list
    for (uint64_t j = rowsLeft; j > 0; j--)
    {
      colNewValueList.push_back(colValueList[(totalRow * i) - j]);
    }
  }

  // end of allocate row id

#ifdef PROFILE
  timer.start("writeColumnRec");
#endif
  // cout << "Writing column record" << endl;

  if (rc == NO_ERROR)
  {
    //----------------------------------------------------------------------
    // Mark extents invalid
    //----------------------------------------------------------------------
    bool successFlag = true;
    unsigned width = 0;
    int curFbo = 0, curBio, lastFbo = -1;

    if (isFirstBatchPm && (totalRow == rowsLeft))
    {
    }
    else
    {
      for (unsigned i = 0; i < colStructList.size(); i++)
      {
        colOp = m_colOp[op(colStructList[i].fCompressionType)];
        width = colStructList[i].colWidth;
        successFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK / width, width, curFbo, curBio);

        if (successFlag)
        {
          if (curFbo != lastFbo)
          {
            RETURN_ON_ERROR(AddLBIDtoList(txnid, colStructList[i], curFbo));
          }
        }
        else
          return ERR_INVALID_PARAM;
      }
    }

    // If we create a new extent for this batch
    for (unsigned i = 0; i < newColStructList.size(); i++)
    {
      colOp = m_colOp[op(newColStructList[i].fCompressionType)];
      width = newColStructList[i].colWidth;
      successFlag = colOp->calculateRowId(lastRidNew, BYTE_PER_BLOCK / width, width, curFbo, curBio);

      if (successFlag)
      {
        if (curFbo != lastFbo)
        {
          RETURN_ON_ERROR(AddLBIDtoList(txnid, newColStructList[i], curFbo));
        }
      }
      else
        return ERR_INVALID_PARAM;
    }

    markTxnExtentsAsInvalid(txnid);

    //----------------------------------------------------------------------
    // Write row(s) to database file(s)
    //----------------------------------------------------------------------
    bool versioning = !(isAutoCommitOn && insertSelect);
    AddDictToList(txnid, dictLbids);
    rc =
        writeColumnRecBinary(txnid, colStructList, colValueList, rowIdArray, newColStructList,
                             colNewValueList, tableOid, useTmpSuffix, versioning);  // @bug 5572 HDFS tmp file
  }

  return rc;
}

int WriteEngineWrapper::insertColumnRec_SYS(const TxnID& txnid, const CSCTypesList& cscColTypeList,
                                            ColStructList& colStructList, ColValueList& colValueList,
                                            DctnryStructList& dctnryStructList, DictStrList& dictStrList,
                                            const int32_t tableOid)
{
  int rc;
  RID* rowIdArray = NULL;
  ColTupleList curTupleList;
  Column curCol;
  ColStruct curColStruct;
  ColValueList colOldValueList;
  ColValueList colNewValueList;
  ColStructList newColStructList;
  DctnryStructList newDctnryStructList;
  HWM hwm = 0;
  HWM newHwm = 0;
  HWM oldHwm = 0;
  ColTupleList::size_type totalRow;
  ColStructList::size_type totalColumns;
  uint64_t rowsLeft = 0;
  bool newExtent = false;
  RIDList ridList;
  ColumnOp* colOp = NULL;
  uint32_t i = 0;
#ifdef PROFILE
  StopWatch timer;
#endif

  m_opType = INSERT;

  // debug information for testing
  if (isDebug(DEBUG_2))
  {
    printInputValue(colStructList, colValueList, ridList, dctnryStructList, dictStrList);
  }

  // end

  // Convert data type and column width to write engine specific
  for (i = 0; i < colStructList.size(); i++)
    Convertor::convertColType(&colStructList[i]);

  rc = checkValid(txnid, colStructList, colValueList, ridList);

  if (rc != NO_ERROR)
    return rc;

  setTransId(txnid);

  curTupleList = static_cast<ColTupleList>(colValueList[0]);
  totalRow = curTupleList.size();
  totalColumns = colStructList.size();
  rowIdArray = new RID[totalRow];
  // use scoped_array to ensure ptr deletion regardless of where we return
  boost::scoped_array<RID> rowIdArrayPtr(rowIdArray);
  memset(rowIdArray, 0, (sizeof(RID) * totalRow));

  // allocate row id(s)
  curColStruct = colStructList[0];
  colOp = m_colOp[op(curColStruct.fCompressionType)];

  colOp->initColumn(curCol);

  // Get the correct segment, partition, column file
  uint16_t dbRoot, segmentNum;
  uint32_t partitionNum;
  vector<ExtentInfo> colExtentInfo;   // Save those empty extents in case of failure to rollback
  vector<ExtentInfo> dictExtentInfo;  // Save those empty extents in case of failure to rollback
  vector<ExtentInfo> fileInfo;
  ExtentInfo info;
  // Don't search for empty space, always append to the end. May need to revisit this part
  dbRoot = curColStruct.fColDbRoot;
  int extState;
  bool extFound;
  RETURN_ON_ERROR(BRMWrapper::getInstance()->getLastHWM_DBroot(curColStruct.dataOid, dbRoot, partitionNum,
                                                               segmentNum, hwm, extState, extFound));

  for (i = 0; i < colStructList.size(); i++)
  {
    colStructList[i].fColPartition = partitionNum;
    colStructList[i].fColSegment = segmentNum;
    colStructList[i].fColDbRoot = dbRoot;
  }

  oldHwm = hwm;  // Save this info for rollback
  // need to pass real dbRoot, partition, and segment to setColParam
  colOp->setColParam(curCol, 0, curColStruct.colWidth, curColStruct.colDataType, curColStruct.colType,
                     curColStruct.dataOid, curColStruct.fCompressionType, dbRoot, partitionNum, segmentNum);
  colOp->findTypeHandler(curColStruct.colWidth, curColStruct.colDataType);
  string segFile;
  rc = colOp->openColumnFile(curCol, segFile, false);  // @bug 5572 HDFS tmp file

  if (rc != NO_ERROR)
  {
    return rc;
  }

  // get hwm first
  // @bug 286 : fix for bug 286 - correct the typo in getHWM
  // RETURN_ON_ERROR(BRMWrapper::getInstance()->getHWM(curColStruct.dataOid, hwm));

  //...Note that we are casting totalRow to int to be in sync with
  //...allocRowId().  So we are assuming that totalRow
  //...(curTupleList.size()) will fit into an int.  We arleady made
  //...that assumption earlier in this method when we used totalRow
  //...in the call to calloc() to allocate rowIdArray.
  Column newCol;
  bool newFile;

#ifdef PROFILE
  timer.start("allocRowId");
#endif

  newColStructList = colStructList;
  newDctnryStructList = dctnryStructList;
  std::vector<boost::shared_ptr<DBRootExtentTracker> > dbRootExtentTrackers;
  bool bUseStartExtent = true;
  rc = colOp->allocRowId(txnid, bUseStartExtent, curCol, (uint64_t)totalRow, rowIdArray, hwm, newExtent,
                         rowsLeft, newHwm, newFile, newColStructList, newDctnryStructList,
                         dbRootExtentTrackers, false, false, 0, false, NULL);

  if ((rc == ERR_FILE_DISK_SPACE) && newExtent)
  {
    for (i = 0; i < newColStructList.size(); i++)
    {
      info.oid = newColStructList[i].dataOid;
      info.partitionNum = newColStructList[i].fColPartition;
      info.segmentNum = newColStructList[i].fColSegment;
      info.dbRoot = newColStructList[i].fColDbRoot;

      if (newFile)
        fileInfo.push_back(info);

      colExtentInfo.push_back(info);
    }

    int rc1 = BRMWrapper::getInstance()->deleteEmptyColExtents(colExtentInfo);

    if ((rc1 == 0) && newFile)
    {
      rc1 = colOp->deleteFile(fileInfo[0].oid, fileInfo[0].dbRoot, fileInfo[0].partitionNum,
                              fileInfo[0].segmentNum);

      if (rc1 != NO_ERROR)
        return rc;

      FileOp fileOp;

      for (i = 0; i < newDctnryStructList.size(); i++)
      {
        if (newDctnryStructList[i].dctnryOid > 0)
        {
          info.oid = newDctnryStructList[i].dctnryOid;
          info.partitionNum = newDctnryStructList[i].fColPartition;
          info.segmentNum = newDctnryStructList[i].fColSegment;
          info.dbRoot = newDctnryStructList[i].fColDbRoot;
          info.newFile = true;
          fileInfo.push_back(info);
          dictExtentInfo.push_back(info);
        }
      }

      if (dictExtentInfo.size() > 0)
      {
        rc1 = BRMWrapper::getInstance()->deleteEmptyDictStoreExtents(dictExtentInfo);

        if (rc1 != NO_ERROR)
          return rc;

        for (unsigned j = 0; j < fileInfo.size(); j++)
        {
          rc1 = fileOp.deleteFile(fileInfo[j].oid, fileInfo[j].dbRoot, fileInfo[j].partitionNum,
                                  fileInfo[j].segmentNum);
        }
      }
    }
  }

  TableMetaData* aTableMetaData = TableMetaData::makeTableMetaData(tableOid);

  //..Expand initial abbreviated extent if any RID in 1st extent is > 256K
  // DMC-SHARED_NOTHING_NOTE: Is it safe to assume only part0 seg0 is abbreviated?
  if ((partitionNum == 0) && (segmentNum == 0) && ((totalRow - rowsLeft) > 0) &&
      (rowIdArray[totalRow - rowsLeft - 1] >= (RID)INITIAL_EXTENT_ROWS_TO_DISK))
  {
    for (size_t k = 1; k < colStructList.size(); k++)
    {
      Column expandCol;
      colOp = m_colOp[op(colStructList[k].fCompressionType)];
      colOp->setColParam(expandCol, 0, colStructList[k].colWidth, colStructList[k].colDataType,
                         colStructList[k].colType, colStructList[k].dataOid,
                         colStructList[k].fCompressionType, dbRoot, partitionNum, segmentNum);
      colOp->findTypeHandler(colStructList[k].colWidth, colStructList[k].colDataType);
      rc = colOp->openColumnFile(expandCol, segFile, false);  // @bug 5572 HDFS tmp file

      if (rc == NO_ERROR)
      {
        if (colOp->abbreviatedExtent(expandCol.dataFile.pFile, colStructList[k].colWidth))
        {
          rc = colOp->expandAbbrevExtent(expandCol);
        }
      }

      if (rc != NO_ERROR)
      {
        if (newExtent)
        {
          // Remove the empty extent added to the first column
          int rc1 = BRMWrapper::getInstance()->deleteEmptyColExtents(colExtentInfo);

          if ((rc1 == 0) && newFile)
          {
            rc1 = colOp->deleteFile(fileInfo[0].oid, fileInfo[0].dbRoot, fileInfo[0].partitionNum,
                                    fileInfo[0].segmentNum);
          }
        }

        colOp->clearColumn(expandCol);  // closes the file
        return rc;
      }

      colOp->clearColumn(expandCol);  // closes the file
    }
  }

  BRMWrapper::setUseVb(true);
  // Tokenize data if needed
  dictStr::iterator dctStr_iter;
  ColTupleList::iterator col_iter;

  for (i = 0; i < colStructList.size(); i++)
  {
    if (colStructList[i].tokenFlag)
    {
      dctStr_iter = dictStrList[i].begin();
      col_iter = colValueList[i].begin();
      Dctnry* dctnry = m_dctnry[op(dctnryStructList[i].fCompressionType)];

      dctnryStructList[i].fColPartition = partitionNum;
      dctnryStructList[i].fColSegment = segmentNum;
      dctnryStructList[i].fColDbRoot = dbRoot;
      rc = dctnry->openDctnry(dctnryStructList[i].dctnryOid, dctnryStructList[i].fColDbRoot,
                              dctnryStructList[i].fColPartition, dctnryStructList[i].fColSegment,
                              false);  // @bug 5572 HDFS tmp file

      if (rc != NO_ERROR)
        return rc;

      ColExtsInfo aColExtsInfo = aTableMetaData->getColExtsInfo(dctnryStructList[i].dctnryOid);
      ColExtsInfo::iterator it = aColExtsInfo.begin();

      while (it != aColExtsInfo.end())
      {
        if ((it->dbRoot == dctnryStructList[i].fColDbRoot) &&
            (it->partNum == dctnryStructList[i].fColPartition) &&
            (it->segNum == dctnryStructList[i].fColSegment))
          break;

        it++;
      }

      if (it == aColExtsInfo.end())  // add this one to the list
      {
        ColExtInfo aExt;
        aExt.dbRoot = dctnryStructList[i].fColDbRoot;
        aExt.partNum = dctnryStructList[i].fColPartition;
        aExt.segNum = dctnryStructList[i].fColSegment;
        aExt.compType = dctnryStructList[i].fCompressionType;
        aExt.isDict = true;
        aColExtsInfo.push_back(aExt);
        aTableMetaData->setColExtsInfo(dctnryStructList[i].dctnryOid, aColExtsInfo);
      }

      for (uint32_t rows = 0; rows < (totalRow - rowsLeft); rows++)
      {
        if (dctStr_iter->length() == 0)
        {
          Token nullToken;
          col_iter->data = nullToken;
        }
        else
        {
#ifdef PROFILE
          timer.start("tokenize");
#endif
          DctnryTuple dctTuple;
          dctTuple.sigValue = (unsigned char*)dctStr_iter->c_str();
          dctTuple.sigSize = dctStr_iter->length();
          dctTuple.isNull = false;
          rc = tokenize(txnid, dctTuple, dctnryStructList[i].fCompressionType);

          if (rc != NO_ERROR)
          {
            dctnry->closeDctnry();
            return rc;
          }

#ifdef PROFILE
          timer.stop("tokenize");
#endif
          col_iter->data = dctTuple.token;
        }

        dctStr_iter++;
        col_iter++;
      }

      // close dictionary files
      rc = dctnry->closeDctnry();

      if (rc != NO_ERROR)
        return rc;

      if (newExtent)
      {
        rc = dctnry->openDctnry(newDctnryStructList[i].dctnryOid, newDctnryStructList[i].fColDbRoot,
                                newDctnryStructList[i].fColPartition, newDctnryStructList[i].fColSegment,
                                false);  // @bug 5572 HDFS tmp file

        if (rc != NO_ERROR)
          return rc;

        aColExtsInfo = aTableMetaData->getColExtsInfo(newDctnryStructList[i].dctnryOid);
        it = aColExtsInfo.begin();

        while (it != aColExtsInfo.end())
        {
          if ((it->dbRoot == newDctnryStructList[i].fColDbRoot) &&
              (it->partNum == newDctnryStructList[i].fColPartition) &&
              (it->segNum == newDctnryStructList[i].fColSegment))
            break;

          it++;
        }

        if (it == aColExtsInfo.end())  // add this one to the list
        {
          ColExtInfo aExt;
          aExt.dbRoot = newDctnryStructList[i].fColDbRoot;
          aExt.partNum = newDctnryStructList[i].fColPartition;
          aExt.segNum = newDctnryStructList[i].fColSegment;
          aExt.compType = newDctnryStructList[i].fCompressionType;
          aExt.isDict = true;
          aColExtsInfo.push_back(aExt);
          aTableMetaData->setColExtsInfo(newDctnryStructList[i].dctnryOid, aColExtsInfo);
        }

        for (uint32_t rows = 0; rows < rowsLeft; rows++)
        {
          if (dctStr_iter->length() == 0)
          {
            Token nullToken;
            col_iter->data = nullToken;
          }
          else
          {
#ifdef PROFILE
            timer.start("tokenize");
#endif
            DctnryTuple dctTuple;
            dctTuple.sigValue = (unsigned char*)dctStr_iter->c_str();
            dctTuple.sigSize = dctStr_iter->length();
            dctTuple.isNull = false;
            rc = tokenize(txnid, dctTuple, newDctnryStructList[i].fCompressionType);

            if (rc != NO_ERROR)
            {
              dctnry->closeDctnry();
              return rc;
            }

#ifdef PROFILE
            timer.stop("tokenize");
#endif
            col_iter->data = dctTuple.token;
          }

          dctStr_iter++;
          col_iter++;
        }

        // close dictionary files
        rc = dctnry->closeDctnry();

        if (rc != NO_ERROR)
          return rc;
      }
    }
  }

  // Update column info structure @Bug 1862 set hwm
  //@Bug 2205 Check whether all rows go to the new extent
  RID lastRid = 0;
  RID lastRidNew = 0;

  if (totalRow - rowsLeft > 0)
  {
    lastRid = rowIdArray[totalRow - rowsLeft - 1];
    lastRidNew = rowIdArray[totalRow - 1];
  }
  else
  {
    lastRid = 0;
    lastRidNew = rowIdArray[totalRow - 1];
  }

  // cout << "rowid allocated is "  << lastRid << endl;
  // if a new extent is created, all the columns in this table should have their own new extent

  //@Bug 1701. Close the file
  m_colOp[op(curCol.compressionType)]->clearColumn(curCol);
  std::vector<BulkSetHWMArg> hwmVecNewext;
  std::vector<BulkSetHWMArg> hwmVecOldext;

  if (newExtent)  // Save all hwms to set them later.
  {
    BulkSetHWMArg aHwmEntryNew;
    BulkSetHWMArg aHwmEntryOld;
    bool succFlag = false;
    unsigned colWidth = 0;
    int extState;
    bool extFound;
    int curFbo = 0, curBio;

    for (i = 0; i < totalColumns; i++)
    {
      Column curColLocal;
      colOp->initColumn(curColLocal);

      colOp = m_colOp[op(newColStructList[i].fCompressionType)];
      colOp->setColParam(curColLocal, 0, newColStructList[i].colWidth, newColStructList[i].colDataType,
                         newColStructList[i].colType, newColStructList[i].dataOid,
                         newColStructList[i].fCompressionType, dbRoot, partitionNum, segmentNum);
      colOp->findTypeHandler(newColStructList[i].colWidth, newColStructList[i].colDataType);

      rc = BRMWrapper::getInstance()->getLastHWM_DBroot(curColLocal.dataFile.fid, dbRoot, partitionNum,
                                                        segmentNum, oldHwm, extState, extFound);

      info.oid = curColLocal.dataFile.fid;
      info.partitionNum = partitionNum;
      info.segmentNum = segmentNum;
      info.dbRoot = dbRoot;
      info.hwm = oldHwm;
      colExtentInfo.push_back(info);
      // @Bug 2714 need to set hwm for the old extent
      colWidth = colStructList[i].colWidth;
      succFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK / colWidth, colWidth, curFbo, curBio);

      // cout << "insertcolumnrec   oid:rid:fbo:hwm = " << colStructList[i].dataOid << ":" << lastRid << ":"
      // << curFbo << ":" << hwm << endl;
      if (succFlag)
      {
        if ((HWM)curFbo > oldHwm)
        {
          aHwmEntryOld.oid = colStructList[i].dataOid;
          aHwmEntryOld.partNum = partitionNum;
          aHwmEntryOld.segNum = segmentNum;
          aHwmEntryOld.hwm = curFbo;
          hwmVecOldext.push_back(aHwmEntryOld);
        }
      }
      else
        return ERR_INVALID_PARAM;

      colWidth = newColStructList[i].colWidth;
      succFlag = colOp->calculateRowId(lastRidNew, BYTE_PER_BLOCK / colWidth, colWidth, curFbo, curBio);

      if (succFlag)
      {
        aHwmEntryNew.oid = newColStructList[i].dataOid;
        aHwmEntryNew.partNum = newColStructList[i].fColPartition;
        aHwmEntryNew.segNum = newColStructList[i].fColSegment;
        aHwmEntryNew.hwm = curFbo;
        hwmVecNewext.push_back(aHwmEntryNew);
      }

      m_colOp[op(curColLocal.compressionType)]->clearColumn(curColLocal);
    }

    // Prepare the valuelist for the new extent
    ColTupleList colTupleList;
    ColTupleList newColTupleList;
    ColTupleList firstPartTupleList;

    for (unsigned i = 0; i < totalColumns; i++)
    {
      colTupleList = static_cast<ColTupleList>(colValueList[i]);

      for (uint64_t j = rowsLeft; j > 0; j--)
      {
        newColTupleList.push_back(colTupleList[totalRow - j]);
      }

      colNewValueList.push_back(newColTupleList);
      newColTupleList.clear();

      // upate the oldvalue list for the old extent
      for (uint64_t j = 0; j < (totalRow - rowsLeft); j++)
      {
        firstPartTupleList.push_back(colTupleList[j]);
      }

      colOldValueList.push_back(firstPartTupleList);
      firstPartTupleList.clear();
    }
  }

  // Mark extents invalid
  bool successFlag = true;
  unsigned width = 0;
  int curFbo = 0, curBio, lastFbo = -1;

  if (totalRow - rowsLeft > 0)
  {
    for (unsigned i = 0; i < colStructList.size(); i++)
    {
      colOp = m_colOp[op(colStructList[i].fCompressionType)];
      width = colStructList[i].colWidth;
      successFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK / width, width, curFbo, curBio);

      if (successFlag)
      {
        if (curFbo != lastFbo)
        {
          RETURN_ON_ERROR(AddLBIDtoList(txnid, colStructList[i], curFbo));
        }
      }
    }
  }

  lastRid = rowIdArray[totalRow - 1];

  for (unsigned i = 0; i < newColStructList.size(); i++)
  {
    colOp = m_colOp[op(newColStructList[i].fCompressionType)];
    width = newColStructList[i].colWidth;
    successFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK / width, width, curFbo, curBio);

    if (successFlag)
    {
      if (curFbo != lastFbo)
      {
        RETURN_ON_ERROR(AddLBIDtoList(txnid, newColStructList[i], curFbo));
      }
    }
  }

  // cout << "lbids size = " << lbids.size()<< endl;
  markTxnExtentsAsInvalid(txnid);

  if (rc == NO_ERROR)
  {
    // MCOL-66 The DBRM can't handle concurrent transactions to sys tables
    static boost::mutex dbrmMutex;
    boost::mutex::scoped_lock lk(dbrmMutex);

    if (newExtent)
    {
      rc = writeColumnRec(txnid, cscColTypeList, colStructList, colOldValueList, rowIdArray, newColStructList,
                          colNewValueList, tableOid, false);  // @bug 5572 HDFS tmp file
    }
    else
    {
      rc = writeColumnRec(txnid, cscColTypeList, colStructList, colValueList, rowIdArray, newColStructList,
                          colNewValueList, tableOid, false);  // @bug 5572 HDFS tmp file
    }
  }

#ifdef PROFILE
  timer.stop("writeColumnRec");
#endif
  //   for (ColTupleList::size_type  i = 0; i < totalRow; i++)
  //      ridList.push_back((RID) rowIdArray[i]);

  // if (rc == NO_ERROR)
  //   rc = flushDataFiles(NO_ERROR);

  if (!newExtent)
  {
    // flushVMCache();
    bool succFlag = false;
    unsigned colWidth = 0;
    int extState;
    bool extFound;
    int curFbo = 0, curBio;
    std::vector<BulkSetHWMArg> hwmVec;

    for (unsigned i = 0; i < totalColumns; i++)
    {
      // colOp = m_colOp[op(colStructList[i].fCompressionType)];
      // Set all columns hwm together
      BulkSetHWMArg aHwmEntry;
      RETURN_ON_ERROR(BRMWrapper::getInstance()->getLastHWM_DBroot(
          colStructList[i].dataOid, dbRoot, partitionNum, segmentNum, hwm, extState, extFound));
      colWidth = colStructList[i].colWidth;
      succFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK / colWidth, colWidth, curFbo, curBio);

      // cout << "insertcolumnrec   oid:rid:fbo:hwm = " << colStructList[i].dataOid << ":" << lastRid << ":"
      // << curFbo << ":" << hwm << endl;
      if (succFlag)
      {
        if ((HWM)curFbo > hwm)
        {
          aHwmEntry.oid = colStructList[i].dataOid;
          aHwmEntry.partNum = partitionNum;
          aHwmEntry.segNum = segmentNum;
          aHwmEntry.hwm = curFbo;
          hwmVec.push_back(aHwmEntry);
        }
      }
      else
        return ERR_INVALID_PARAM;
    }

    if (hwmVec.size() > 0)
    {
      std::vector<BRM::CPInfoMerge> mergeCPDataArgs;
      RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP(hwmVec, mergeCPDataArgs));
    }
  }

  if (newExtent)
  {
#ifdef PROFILE
    timer.start("flushVMCache");
#endif
    std::vector<BRM::CPInfoMerge> mergeCPDataArgs;
    RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP(hwmVecNewext, mergeCPDataArgs));
    RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP(hwmVecOldext, mergeCPDataArgs));
    // flushVMCache();
#ifdef PROFILE
    timer.stop("flushVMCache");
#endif
  }

#ifdef PROFILE
  timer.finish();
#endif
  return rc;
}

int WriteEngineWrapper::insertColumnRec_Single(const TxnID& txnid, const CSCTypesList& cscColTypeList,
                                               ColStructList& colStructList, ColValueList& colValueList,
                                               DctnryStructList& dctnryStructList, DictStrList& dictStrList,
                                               const int32_t tableOid)
{
  int rc;
  RID* rowIdArray = NULL;
  ColTupleList curTupleList;
  Column curCol;
  ColStruct curColStruct;
  ColValueList colOldValueList;
  ColValueList colNewValueList;
  ColStructList newColStructList;
  DctnryStructList newDctnryStructList;
  HWM hwm = 0;
  HWM newHwm = 0;
  HWM oldHwm = 0;
  ColTupleList::size_type totalRow;
  ColStructList::size_type totalColumns;
  uint64_t rowsLeft = 0;
  bool newExtent = false;
  RIDList ridList;
  ColumnOp* colOp = NULL;
  uint32_t i = 0;

#ifdef PROFILE
  StopWatch timer;
#endif

  m_opType = INSERT;

  // debug information for testing
  if (isDebug(DEBUG_2))
  {
    printInputValue(colStructList, colValueList, ridList, dctnryStructList, dictStrList);
  }

  // Convert data type and column width to write engine specific
  for (i = 0; i < colStructList.size(); i++)
    Convertor::convertColType(&colStructList[i]);

  uint32_t colId = 0;
  // MCOL-1675: find the smallest column width to calculate the RowID from so
  // that all HWMs will be incremented by this operation
  findSmallestColumn(colId, colStructList);

  rc = checkValid(txnid, colStructList, colValueList, ridList);

  if (rc != NO_ERROR)
    return rc;

  setTransId(txnid);

  curTupleList = static_cast<ColTupleList>(colValueList[0]);
  totalRow = curTupleList.size();
  totalColumns = colStructList.size();
  rowIdArray = new RID[totalRow];
  // use scoped_array to ensure ptr deletion regardless of where we return
  boost::scoped_array<RID> rowIdArrayPtr(rowIdArray);
  memset(rowIdArray, 0, (sizeof(RID) * totalRow));

  //--------------------------------------------------------------------------
  // allocate row id(s)
  //--------------------------------------------------------------------------
  curColStruct = colStructList[colId];
  colOp = m_colOp[op(curColStruct.fCompressionType)];

  colOp->initColumn(curCol);

  // Get the correct segment, partition, column file
  uint16_t dbRoot;
  uint16_t segmentNum = 0;
  uint32_t partitionNum = 0;
  // Don't search for empty space, always append to the end. May revisit later
  dbRoot = curColStruct.fColDbRoot;
  int extState;
  bool bStartExtFound;
  bool bUseStartExtent = false;
  RETURN_ON_ERROR(BRMWrapper::getInstance()->getLastHWM_DBroot(curColStruct.dataOid, dbRoot, partitionNum,
                                                               segmentNum, hwm, extState, bStartExtFound));

  if ((bStartExtFound) && (extState == BRM::EXTENTAVAILABLE))
    bUseStartExtent = true;

  for (i = 0; i < colStructList.size(); i++)
  {
    colStructList[i].fColPartition = partitionNum;
    colStructList[i].fColSegment = segmentNum;
    colStructList[i].fColDbRoot = dbRoot;
  }

  for (i = 0; i < dctnryStructList.size(); i++)
  {
    dctnryStructList[i].fColPartition = partitionNum;
    dctnryStructList[i].fColSegment = segmentNum;
    dctnryStructList[i].fColDbRoot = dbRoot;
  }

  oldHwm = hwm;  // Save this info for rollback
  // need to pass real dbRoot, partition, and segment to setColParam
  colOp->setColParam(curCol, colId, curColStruct.colWidth, curColStruct.colDataType, curColStruct.colType,
                     curColStruct.dataOid, curColStruct.fCompressionType, dbRoot, partitionNum, segmentNum);
  colOp->findTypeHandler(curColStruct.colWidth, curColStruct.colDataType);
  string segFile;

  if (bUseStartExtent)
  {
    rc = colOp->openColumnFile(curCol, segFile, true);  // @bug 5572 HDFS tmp file

    if (rc != NO_ERROR)
    {
      return rc;
    }
  }

  bool newFile;

#ifdef PROFILE
  timer.start("allocRowId");
#endif
  newColStructList = colStructList;
  newDctnryStructList = dctnryStructList;
  std::vector<boost::shared_ptr<DBRootExtentTracker> > dbRootExtentTrackers;
  rc = colOp->allocRowId(txnid, bUseStartExtent, curCol, (uint64_t)totalRow, rowIdArray, hwm, newExtent,
                         rowsLeft, newHwm, newFile, newColStructList, newDctnryStructList,
                         dbRootExtentTrackers, false, false, 0, false, NULL);

  //--------------------------------------------------------------------------
  // Handle case where we ran out of disk space allocating a new extent.
  // Rollback extentmap and delete any db files that were created.
  //--------------------------------------------------------------------------
  if (rc != NO_ERROR)
  {
    if ((rc == ERR_FILE_DISK_SPACE) && newExtent)
    {
      vector<ExtentInfo> colExtentInfo;
      vector<ExtentInfo> dictExtentInfo;
      vector<ExtentInfo> fileInfo;
      ExtentInfo info;

      for (i = 0; i < newColStructList.size(); i++)
      {
        info.oid = newColStructList[i].dataOid;
        info.partitionNum = newColStructList[i].fColPartition;
        info.segmentNum = newColStructList[i].fColSegment;
        info.dbRoot = newColStructList[i].fColDbRoot;

        if (newFile)
          fileInfo.push_back(info);

        colExtentInfo.push_back(info);
      }

      int rc1 = BRMWrapper::getInstance()->deleteEmptyColExtents(colExtentInfo);

      // Only rollback dictionary extents "if" store file is new
      if ((rc1 == 0) && newFile)
      {
        for (unsigned int j = 0; j < fileInfo.size(); j++)
        {
          // ignore return code and delete what we can
          rc1 = colOp->deleteFile(fileInfo[j].oid, fileInfo[j].dbRoot, fileInfo[j].partitionNum,
                                  fileInfo[j].segmentNum);
        }

        fileInfo.clear();

        for (i = 0; i < newDctnryStructList.size(); i++)
        {
          if (newDctnryStructList[i].dctnryOid > 0)
          {
            info.oid = newDctnryStructList[i].dctnryOid;
            info.partitionNum = newDctnryStructList[i].fColPartition;
            info.segmentNum = newDctnryStructList[i].fColSegment;
            info.dbRoot = newDctnryStructList[i].fColDbRoot;
            info.newFile = true;
            fileInfo.push_back(info);
            dictExtentInfo.push_back(info);
          }
        }

        if (dictExtentInfo.size() > 0)
        {
          FileOp fileOp;
          rc1 = BRMWrapper::getInstance()->deleteEmptyDictStoreExtents(dictExtentInfo);

          if (rc1 != NO_ERROR)
            return rc;

          for (unsigned j = 0; j < fileInfo.size(); j++)
          {
            rc1 = fileOp.deleteFile(fileInfo[j].oid, fileInfo[j].dbRoot, fileInfo[j].partitionNum,
                                    fileInfo[j].segmentNum);
          }
        }
      }
    }  // disk space error allocating new extent

    return rc;
  }  // rc != NO_ERROR from call to allocRowID()

#ifdef PROFILE
  timer.stop("allocRowId");
#endif

  TableMetaData* aTableMetaData = TableMetaData::makeTableMetaData(tableOid);

  //--------------------------------------------------------------------------
  // Expand initial abbreviated extent if any RID in 1st extent is > 256K.
  // if totalRow == rowsLeft, then not adding rows to 1st extent, so skip it.
  //--------------------------------------------------------------------------
  // DMC-SHARED_NOTHING_NOTE: Is it safe to assume only part0 seg0 is abbreviated?
  if ((colStructList[colId].fColPartition == 0) && (colStructList[colId].fColSegment == 0) &&
      ((totalRow - rowsLeft) > 0) &&
      (rowIdArray[totalRow - rowsLeft - 1] >= (RID)INITIAL_EXTENT_ROWS_TO_DISK))
  {
    for (unsigned k = 0; k < colStructList.size(); k++)
    {
      if (k == colId)
        continue;
      Column expandCol;
      colOp = m_colOp[op(colStructList[k].fCompressionType)];
      colOp->setColParam(expandCol, 0, colStructList[k].colWidth, colStructList[k].colDataType,
                         colStructList[k].colType, colStructList[k].dataOid,
                         colStructList[k].fCompressionType, colStructList[k].fColDbRoot,
                         colStructList[k].fColPartition, colStructList[k].fColSegment);
      colOp->findTypeHandler(colStructList[k].colWidth, colStructList[k].colDataType);
      rc = colOp->openColumnFile(expandCol, segFile, true);  // @bug 5572 HDFS tmp file

      if (rc == NO_ERROR)
      {
        if (colOp->abbreviatedExtent(expandCol.dataFile.pFile, colStructList[k].colWidth))
        {
          rc = colOp->expandAbbrevExtent(expandCol);
        }
      }

      colOp->clearColumn(expandCol);  // closes the file

      if (rc != NO_ERROR)
      {
        return rc;
      }
    }  // loop through columns
  }    // if starting extent needs to be expanded

  //--------------------------------------------------------------------------
  // Tokenize data if needed
  //--------------------------------------------------------------------------
  dictStr::iterator dctStr_iter;
  ColTupleList::iterator col_iter;

  for (unsigned i = 0; i < colStructList.size(); i++)
  {
    if (colStructList[i].tokenFlag)
    {
      dctStr_iter = dictStrList[i].begin();
      col_iter = colValueList[i].begin();
      Dctnry* dctnry = m_dctnry[op(dctnryStructList[i].fCompressionType)];

      ColExtsInfo aColExtsInfo = aTableMetaData->getColExtsInfo(dctnryStructList[i].dctnryOid);
      ColExtsInfo::iterator it = aColExtsInfo.begin();

      if (bUseStartExtent)
      {
        rc = dctnry->openDctnry(dctnryStructList[i].dctnryOid, dctnryStructList[i].fColDbRoot,
                                dctnryStructList[i].fColPartition, dctnryStructList[i].fColSegment,
                                true);  // @bug 5572 HDFS tmp file

        if (rc != NO_ERROR)
          return rc;

        while (it != aColExtsInfo.end())
        {
          if ((it->dbRoot == dctnryStructList[i].fColDbRoot) &&
              (it->partNum == dctnryStructList[i].fColPartition) &&
              (it->segNum == dctnryStructList[i].fColSegment))
            break;

          it++;
        }

        if (it == aColExtsInfo.end())  // add this one to the list
        {
          ColExtInfo aExt;
          aExt.dbRoot = dctnryStructList[i].fColDbRoot;
          aExt.partNum = dctnryStructList[i].fColPartition;
          aExt.segNum = dctnryStructList[i].fColSegment;
          aExt.compType = dctnryStructList[i].fCompressionType;
          aExt.isDict = true;
          aColExtsInfo.push_back(aExt);
          aTableMetaData->setColExtsInfo(dctnryStructList[i].dctnryOid, aColExtsInfo);
        }

        for (uint32_t rows = 0; rows < (totalRow - rowsLeft); rows++)
        {
          if (dctStr_iter->length() == 0)
          {
            Token nullToken;
            col_iter->data = nullToken;
          }
          else
          {
#ifdef PROFILE
            timer.start("tokenize");
#endif
            DctnryTuple dctTuple;
            dctTuple.sigValue = (unsigned char*)dctStr_iter->c_str();
            dctTuple.sigSize = dctStr_iter->length();
            dctTuple.isNull = false;
            rc = tokenize(txnid, dctTuple, dctnryStructList[i].fCompressionType);

            if (rc != NO_ERROR)
            {
              dctnry->closeDctnry();
              return rc;
            }

#ifdef PROFILE
            timer.stop("tokenize");
#endif
            col_iter->data = dctTuple.token;
          }

          dctStr_iter++;
          col_iter++;
        }

        // close dictionary files
        rc = dctnry->closeDctnry();

        if (rc != NO_ERROR)
          return rc;
      }  // tokenize dictionary rows in 1st extent

      if (newExtent)
      {
        rc = dctnry->openDctnry(newDctnryStructList[i].dctnryOid, newDctnryStructList[i].fColDbRoot,
                                newDctnryStructList[i].fColPartition, newDctnryStructList[i].fColSegment,
                                false);  // @bug 5572 HDFS tmp file

        if (rc != NO_ERROR)
          return rc;

        aColExtsInfo = aTableMetaData->getColExtsInfo(newDctnryStructList[i].dctnryOid);
        it = aColExtsInfo.begin();

        while (it != aColExtsInfo.end())
        {
          if ((it->dbRoot == newDctnryStructList[i].fColDbRoot) &&
              (it->partNum == newDctnryStructList[i].fColPartition) &&
              (it->segNum == newDctnryStructList[i].fColSegment))
            break;

          it++;
        }

        if (it == aColExtsInfo.end())  // add this one to the list
        {
          ColExtInfo aExt;
          aExt.dbRoot = newDctnryStructList[i].fColDbRoot;
          aExt.partNum = newDctnryStructList[i].fColPartition;
          aExt.segNum = newDctnryStructList[i].fColSegment;
          aExt.compType = newDctnryStructList[i].fCompressionType;
          aExt.isDict = true;
          aColExtsInfo.push_back(aExt);
          aTableMetaData->setColExtsInfo(newDctnryStructList[i].dctnryOid, aColExtsInfo);
        }

        for (uint32_t rows = 0; rows < rowsLeft; rows++)
        {
          if (dctStr_iter->length() == 0)
          {
            Token nullToken;
            col_iter->data = nullToken;
          }
          else
          {
#ifdef PROFILE
            timer.start("tokenize");
#endif
            DctnryTuple dctTuple;
            dctTuple.sigValue = (unsigned char*)dctStr_iter->c_str();
            dctTuple.sigSize = dctStr_iter->length();
            dctTuple.isNull = false;
            rc = tokenize(txnid, dctTuple, newDctnryStructList[i].fCompressionType);

            if (rc != NO_ERROR)
            {
              dctnry->closeDctnry();
              return rc;
            }

#ifdef PROFILE
            timer.stop("tokenize");
#endif
            col_iter->data = dctTuple.token;
          }

          dctStr_iter++;
          col_iter++;
        }

        // close dictionary files
        rc = dctnry->closeDctnry();

        if (rc != NO_ERROR)
          return rc;
      }  // tokenize dictionary rows in second extent
    }    // tokenize dictionary columns
  }      // loop through columns to see which ones need tokenizing

  //----------------------------------------------------------------------
  // Update column info structure @Bug 1862 set hwm, and
  // Prepare ValueList for new extent (if applicable)
  //----------------------------------------------------------------------
  //@Bug 2205 Check whether all rows go to the new extent
  RID lastRid = 0;
  RID lastRidNew = 0;

  if (totalRow - rowsLeft > 0)
  {
    lastRid = rowIdArray[totalRow - rowsLeft - 1];
    lastRidNew = rowIdArray[totalRow - 1];
  }
  else
  {
    lastRid = 0;
    lastRidNew = rowIdArray[totalRow - 1];
  }

  // if a new extent is created, all the columns in this table should
  // have their own new extent

  //@Bug 1701. Close the file
  if (bUseStartExtent)
  {
    m_colOp[op(curCol.compressionType)]->clearColumn(curCol);
  }

  std::vector<BulkSetHWMArg> hwmVecNewext;
  std::vector<BulkSetHWMArg> hwmVecOldext;

  if (newExtent)  // Save all hwms to set them later.
  {
    BulkSetHWMArg aHwmEntryNew;
    BulkSetHWMArg aHwmEntryOld;

    bool succFlag = false;
    unsigned colWidth = 0;
    int curFbo = 0, curBio;

    for (i = 0; i < totalColumns; i++)
    {
      colOp = m_colOp[op(newColStructList[i].fCompressionType)];

      // @Bug 2714 need to set hwm for the old extent
      colWidth = colStructList[i].colWidth;
      succFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK / colWidth, colWidth, curFbo, curBio);

      if (succFlag)
      {
        if ((HWM)curFbo > oldHwm)
        {
          aHwmEntryOld.oid = colStructList[i].dataOid;
          aHwmEntryOld.partNum = colStructList[i].fColPartition;
          aHwmEntryOld.segNum = colStructList[i].fColSegment;
          aHwmEntryOld.hwm = curFbo;
          hwmVecOldext.push_back(aHwmEntryOld);
        }
      }
      else
        return ERR_INVALID_PARAM;

      colWidth = newColStructList[i].colWidth;
      succFlag = colOp->calculateRowId(lastRidNew, BYTE_PER_BLOCK / colWidth, colWidth, curFbo, curBio);

      if (succFlag)
      {
        aHwmEntryNew.oid = newColStructList[i].dataOid;
        aHwmEntryNew.partNum = newColStructList[i].fColPartition;
        aHwmEntryNew.segNum = newColStructList[i].fColSegment;
        aHwmEntryNew.hwm = curFbo;
        hwmVecNewext.push_back(aHwmEntryNew);
      }
    }

    //----------------------------------------------------------------------
    // Prepare the valuelist for the new extent
    //----------------------------------------------------------------------
    ColTupleList colTupleList;
    ColTupleList newColTupleList;
    ColTupleList firstPartTupleList;

    for (unsigned i = 0; i < totalColumns; i++)
    {
      colTupleList = static_cast<ColTupleList>(colValueList[i]);

      for (uint64_t j = rowsLeft; j > 0; j--)
      {
        newColTupleList.push_back(colTupleList[totalRow - j]);
      }

      colNewValueList.push_back(newColTupleList);

      newColTupleList.clear();

      // upate the oldvalue list for the old extent
      for (uint64_t j = 0; j < (totalRow - rowsLeft); j++)
      {
        firstPartTupleList.push_back(colTupleList[j]);
      }

      colOldValueList.push_back(firstPartTupleList);
      firstPartTupleList.clear();
    }
  }

  //--------------------------------------------------------------------------
  // Mark extents invalid
  //--------------------------------------------------------------------------
  // WIP
  // Set min/max in dmlprocprocessor if aplicable
  vector<BRM::LBID_t> lbids;
  vector<CalpontSystemCatalog::ColDataType> colDataTypes;
  bool successFlag = true;
  unsigned width = 0;
  int curFbo = 0, curBio, lastFbo = -1;
  lastRid = rowIdArray[totalRow - 1];

  for (unsigned i = 0; i < colStructList.size(); i++)
  {
    colOp = m_colOp[op(colStructList[i].fCompressionType)];
    width = colStructList[i].colWidth;
    successFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK / width, width, curFbo, curBio);

    if (successFlag)
    {
      if (curFbo != lastFbo)
      {
        colDataTypes.push_back(colStructList[i].colDataType);
        RETURN_ON_ERROR(AddLBIDtoList(txnid, colStructList[i], curFbo));
      }
    }
  }

  markTxnExtentsAsInvalid(txnid);

  //--------------------------------------------------------------------------
  // Write row(s) to database file(s)
  //--------------------------------------------------------------------------
#ifdef PROFILE
  timer.start("writeColumnRec");
#endif

  if (rc == NO_ERROR)
  {
    if (newExtent)
    {
      rc = writeColumnRec(txnid, cscColTypeList, colStructList, colOldValueList, rowIdArray, newColStructList,
                          colNewValueList, tableOid,
                          false);  // @bug 5572 HDFS tmp file
    }
    else
    {
      rc = writeColumnRec(txnid, cscColTypeList, colStructList, colValueList, rowIdArray, newColStructList,
                          colNewValueList, tableOid,
                          true);  // @bug 5572 HDFS tmp file
    }
  }

#ifdef PROFILE
  timer.stop("writeColumnRec");
#endif

  //--------------------------------------------------------------------------
  // Update BRM
  //--------------------------------------------------------------------------
  if (!newExtent)
  {
    bool succFlag = false;
    unsigned colWidth = 0;
    int extState;
    bool extFound;
    int curFbo = 0, curBio;
    std::vector<BulkSetHWMArg> hwmVec;

    for (unsigned i = 0; i < totalColumns; i++)
    {
      // Set all columns hwm together
      BulkSetHWMArg aHwmEntry;
      RETURN_ON_ERROR(BRMWrapper::getInstance()->getLastHWM_DBroot(
          colStructList[i].dataOid, colStructList[i].fColDbRoot, colStructList[i].fColPartition,
          colStructList[i].fColSegment, hwm, extState, extFound));
      colWidth = colStructList[i].colWidth;
      succFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK / colWidth, colWidth, curFbo, curBio);

      // cout << "insertcolumnrec   oid:rid:fbo:hwm = " <<
      // colStructList[i].dataOid << ":" << lastRid << ":" <<
      // curFbo << ":" << hwm << endl;
      if (succFlag)
      {
        if ((HWM)curFbo > hwm)
        {
          aHwmEntry.oid = colStructList[i].dataOid;
          aHwmEntry.partNum = colStructList[i].fColPartition;
          aHwmEntry.segNum = colStructList[i].fColSegment;
          aHwmEntry.hwm = curFbo;
          hwmVec.push_back(aHwmEntry);
        }
      }
      else
        return ERR_INVALID_PARAM;
    }

    std::vector<BRM::CPInfoMerge> mergeCPDataArgs;
    RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP(hwmVec, mergeCPDataArgs));
  }
  else  // if (newExtent)
  {
#ifdef PROFILE
    timer.start("flushVMCache");
#endif
    std::vector<BRM::CPInfoMerge> mergeCPDataArgs;

    if (hwmVecNewext.size() > 0)
      RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP(hwmVecNewext, mergeCPDataArgs));

    if (hwmVecOldext.size() > 0)
      RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP(hwmVecOldext, mergeCPDataArgs));

#ifdef PROFILE
    timer.stop("flushVMCache");
#endif
  }

#ifdef PROFILE
  timer.finish();
#endif
  return rc;
}

/*@brief printInputValue - Print input value
 */
/***********************************************************
 * DESCRIPTION:
 *    Print input value
 * PARAMETERS:
 *    tableOid - table object id
 *    colStructList - column struct list
 *    colValueList - column value list
 *    ridList - RID list
 * RETURN:
 *    none
 ***********************************************************/
void WriteEngineWrapper::printInputValue(const ColStructList& colStructList, const ColValueList& colValueList,
                                         const RIDList& ridList, const DctnryStructList& dctnryStructList,
                                         const DictStrList& dictStrList) const
{
  ColTupleList curTupleList;
  ColStruct curColStruct;
  ColTuple curTuple;
  string curStr;
  ColStructList::size_type i;
  size_t j;
  OidToIdxMap oidToIdxMap;

  std::cerr << std::endl << "=========================" << std::endl;

  std::cerr << "Total RIDs: " << ridList.size() << std::endl;

  for (i = 0; i < ridList.size(); i++)
    std::cerr << "RID[" << i << "] : " << ridList[i] << std::endl;

  std::cerr << "Total Columns: " << colStructList.size() << std::endl;

  for (i = 0; i < colStructList.size(); i++)
  {
    curColStruct = colStructList[i];
    curTupleList = colValueList[i];
    if (curColStruct.tokenFlag)
    {
      oidToIdxMap.insert({curColStruct.dataOid, i});
      continue;
    }

    std::cerr << "Column[" << i << "]";
    std::cerr << "Data file OID : " << curColStruct.dataOid << "\t";
    std::cerr << "Width : " << curColStruct.colWidth << "\t"
              << " Type: " << curColStruct.colDataType << std::endl;
    std::cerr << "Total values : " << curTupleList.size() << std::endl;

    for (j = 0; j < curTupleList.size(); j++)
    {
      curTuple = curTupleList[j];

      try
      {
        if (curTuple.data.type() == typeid(int))
          curStr = boost::lexical_cast<string>(boost::any_cast<int>(curTuple.data));
        else if (curTuple.data.type() == typeid(unsigned int))
          curStr = boost::lexical_cast<string>(boost::any_cast<unsigned int>(curTuple.data));
        else if (curTuple.data.type() == typeid(float))
          curStr = boost::lexical_cast<string>(boost::any_cast<float>(curTuple.data));
        else if (curTuple.data.type() == typeid(long long))
          curStr = boost::lexical_cast<string>(boost::any_cast<long long>(curTuple.data));
        else if (curTuple.data.type() == typeid(unsigned long long))
          curStr = boost::lexical_cast<string>(boost::any_cast<unsigned long long>(curTuple.data));
        else if (curTuple.data.type() == typeid(int128_t))
          curStr = datatypes::TSInt128(boost::any_cast<int128_t>(curTuple.data)).toString();
        else if (curTuple.data.type() == typeid(double))
          curStr = boost::lexical_cast<string>(boost::any_cast<double>(curTuple.data));
        else if (curTuple.data.type() == typeid(short))
          curStr = boost::lexical_cast<string>(boost::any_cast<short>(curTuple.data));
        else if (curTuple.data.type() == typeid(unsigned short))
          curStr = boost::lexical_cast<string>(boost::any_cast<unsigned short>(curTuple.data));
        else if (curTuple.data.type() == typeid(char))
          curStr = boost::lexical_cast<string>(boost::any_cast<char>(curTuple.data));
        else
          curStr = boost::any_cast<string>(curTuple.data);
      }
      catch (...)
      {
      }

      if (isDebug(DEBUG_3))
        std::cerr << "Value[" << j << "]: " << curStr.c_str() << std::endl;
    }
  }
  for (i = 0; i < dctnryStructList.size(); ++i)
  {
    if (dctnryStructList[i].dctnryOid == 0)
      continue;
    std::cerr << "Dict[" << i << "]";
    std::cerr << " file OID : " << dctnryStructList[i].dctnryOid
              << " Token file OID: " << dctnryStructList[i].columnOid << "\t";
    std::cerr << "Width : " << dctnryStructList[i].colWidth << "\t"
              << " Type: " << dctnryStructList[i].fCompressionType << std::endl;
    std::cerr << "Total values : " << dictStrList.size() << std::endl;
    if (isDebug(DEBUG_3))
    {
      for (j = 0; j < dictStrList[i].size(); ++j)
      {
        // We presume there will be a value.
        auto tokenOidIdx = oidToIdxMap[dctnryStructList[i].columnOid];
        std::cerr << "string [" << dictStrList[i][j] << "]" << std::endl;
        bool isToken = colStructList[tokenOidIdx].colType == WriteEngine::WR_TOKEN &&
                       colStructList[tokenOidIdx].tokenFlag;
        if (isToken && !colValueList[tokenOidIdx][j].data.empty())
        {
          Token t = boost::any_cast<Token>(colValueList[tokenOidIdx][j].data);
          std::cerr << "Token: block pos:[" << t.op << "] fbo: [" << t.fbo << "] bc: [" << t.bc << "]"
                    << std::endl;
        }
      }
    }
  }

  std::cerr << "=========================" << std::endl;
}

/***********************************************************
 * DESCRIPTION:
 *    Process version buffer before any write operation
 * PARAMETERS:
 *    txnid - transaction id
 *    oid - column oid
 *    totalRow - total number of rows
 *    rowIdArray - rowid array
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in inserting the value
 ***********************************************************/
int WriteEngineWrapper::processVersionBuffer(IDBDataFile* pFile, const TxnID& txnid,
                                             const ColStruct& colStruct, int width, int totalRow,
                                             const RID* rowIdArray, vector<LBIDRange>& rangeList)
{
  if (idbdatafile::IDBPolicy::useHdfs())
    return 0;

  RID curRowId;
  int rc = NO_ERROR;
  int curFbo = 0, curBio, lastFbo = -1;
  bool successFlag;
  BRM::LBID_t lbid;
  BRM::VER_t verId = (BRM::VER_t)txnid;
  vector<uint32_t> fboList;
  LBIDRange range;
  ColumnOp* colOp = m_colOp[op(colStruct.fCompressionType)];

  for (int i = 0; i < totalRow; i++)
  {
    curRowId = rowIdArray[i];
    // cout << "processVersionBuffer got rid " << curRowId << endl;
    successFlag = colOp->calculateRowId(curRowId, BYTE_PER_BLOCK / width, width, curFbo, curBio);

    if (successFlag)
    {
      if (curFbo != lastFbo)
      {
        // cout << "processVersionBuffer is processing lbid  " << lbid << endl;
        RETURN_ON_ERROR(BRMWrapper::getInstance()->getBrmInfo(colStruct.dataOid, colStruct.fColPartition,
                                                              colStruct.fColSegment, curFbo, lbid));
        // cout << "processVersionBuffer is processing lbid  " << lbid << endl;
        fboList.push_back((uint32_t)curFbo);
        range.start = lbid;
        range.size = 1;
        rangeList.push_back(range);
      }

      lastFbo = curFbo;
    }
  }

  std::vector<VBRange> freeList;
  rc = BRMWrapper::getInstance()->writeVB(pFile, verId, colStruct.dataOid, fboList, rangeList, colOp,
                                          freeList, colStruct.fColDbRoot);

  return rc;
}

int WriteEngineWrapper::processVersionBuffers(IDBDataFile* pFile, const TxnID& txnid,
                                              const ColStruct& colStruct, int width, int totalRow,
                                              const RIDList& ridList, vector<LBIDRange>& rangeList)
{
  if (idbdatafile::IDBPolicy::useHdfs())
    return 0;

  RID curRowId;
  int rc = NO_ERROR;
  int curFbo = 0, curBio, lastFbo = -1;
  bool successFlag;
  BRM::LBID_t lbid;
  BRM::VER_t verId = (BRM::VER_t)txnid;
  LBIDRange range;
  vector<uint32_t> fboList;
  // vector<LBIDRange>   rangeList;
  ColumnOp* colOp = m_colOp[op(colStruct.fCompressionType)];

  for (int i = 0; i < totalRow; i++)
  {
    curRowId = ridList[i];
    // cout << "processVersionBuffer got rid " << curRowId << endl;
    successFlag = colOp->calculateRowId(curRowId, BYTE_PER_BLOCK / width, width, curFbo, curBio);

    if (successFlag)
    {
      if (curFbo != lastFbo)
      {
        // cout << "processVersionBuffer is processing lbid  " << lbid << endl;
        RETURN_ON_ERROR(BRMWrapper::getInstance()->getBrmInfo(colStruct.dataOid, colStruct.fColPartition,
                                                              colStruct.fColSegment, curFbo, lbid));
        // cout << "processVersionBuffer is processing lbid  " << lbid << endl;
        fboList.push_back((uint32_t)curFbo);
        range.start = lbid;
        range.size = 1;
        rangeList.push_back(range);
      }

      lastFbo = curFbo;
    }
  }

  // cout << "calling writeVB with blocks " << rangeList.size() << endl;
  std::vector<VBRange> freeList;
  rc = BRMWrapper::getInstance()->writeVB(pFile, verId, colStruct.dataOid, fboList, rangeList, colOp,
                                          freeList, colStruct.fColDbRoot);

  return rc;
}

int WriteEngineWrapper::processBeginVBCopy(const TxnID& txnid, const vector<ColStruct>& colStructList,
                                           const RIDList& ridList, std::vector<VBRange>& freeList,
                                           vector<vector<uint32_t> >& fboLists,
                                           vector<vector<LBIDRange> >& rangeLists,
                                           vector<LBIDRange>& rangeListTot)
{
  if (idbdatafile::IDBPolicy::useHdfs())
    return 0;

  RID curRowId;
  int rc = NO_ERROR;
  int curFbo = 0, curBio, lastFbo = -1;
  bool successFlag;
  BRM::LBID_t lbid;
  LBIDRange range;

  // StopWatch timer;
  // timer.start("calculation");
  for (uint32_t j = 0; j < colStructList.size(); j++)
  {
    vector<uint32_t> fboList;
    vector<LBIDRange> rangeList;
    lastFbo = -1;
    ColumnOp* colOp = m_colOp[op(colStructList[j].fCompressionType)];

    ColStruct curColStruct = colStructList[j];
    Convertor::convertColType(&curColStruct);

    for (uint32_t i = 0; i < ridList.size(); i++)
    {
      curRowId = ridList[i];
      // cout << "processVersionBuffer got rid " << curRowId << endl;
      successFlag = colOp->calculateRowId(curRowId, BYTE_PER_BLOCK / curColStruct.colWidth,
                                          curColStruct.colWidth, curFbo, curBio);

      if (successFlag)
      {
        if (curFbo != lastFbo)
        {
          // cout << "processVersionBuffer is processing curFbo  " << curFbo << endl;
          RETURN_ON_ERROR(BRMWrapper::getInstance()->getBrmInfo(colStructList[j].dataOid,
                                                                colStructList[j].fColPartition,
                                                                colStructList[j].fColSegment, curFbo, lbid));
          // cout << "beginVBCopy is processing lbid:transaction  " << lbid <<":"<<txnid<< endl;
          fboList.push_back((uint32_t)curFbo);
          range.start = lbid;
          range.size = 1;
          rangeList.push_back(range);
        }

        lastFbo = curFbo;
      }
    }

    BRMWrapper::getInstance()->pruneLBIDList(txnid, &rangeList, &fboList);
    rangeLists.push_back(rangeList);

    fboLists.push_back(fboList);
    rangeListTot.insert(rangeListTot.end(), rangeList.begin(), rangeList.end());
  }

  if (rangeListTot.size() > 0)
    rc = BRMWrapper::getInstance()->getDbrmObject()->beginVBCopy(txnid, colStructList[0].fColDbRoot,
                                                                 rangeListTot, freeList);

  // timer.stop("beginVBCopy");
  // timer.finish();
  return rc;
}

/**
 * @brief Process versioning for batch insert - only version the hwm block.
 */
#if 0
int WriteEngineWrapper::processBatchVersions(const TxnID& txnid, std::vector<Column> columns, std::vector<BRM::LBIDRange>&   rangeList)
{
    int rc = 0;
    std::vector<DbFileOp*> fileOps;

    //open the column files
    for ( unsigned i = 0; i < columns.size(); i++)
    {
        ColumnOp* colOp = m_colOp[op(columns[i].compressionType)];
        Column curCol;
        // set params
        colOp->initColumn(curCol);
        ColType colType;
        Convertor::convertColType(columns[i].colDataType, colType);
        colOp->setColParam(curCol, 0, columns[i].colWidth,
                           columns[i].colDataType, colType, columns[i].dataFile.oid,
                           columns[i].compressionType,
                           columns[i].dataFile.fDbRoot, columns[i].dataFile.fPartition, columns[i].dataFile.fSegment);
        string segFile;
        rc = colOp->openColumnFile(curCol, segFile, IO_BUFF_SIZE);

        if (rc != NO_ERROR)
            break;

        columns[i].dataFile.pFile = curCol.dataFile.pFile;
        fileOps.push_back(colOp);
    }

    if ( rc == 0)
    {
        BRM::VER_t  verId = (BRM::VER_t) txnid;
        rc = BRMWrapper::getInstance()->writeBatchVBs(verId, columns, rangeList, fileOps);
    }

    //close files
    for ( unsigned i = 0; i < columns.size(); i++)
    {
        ColumnOp* colOp = dynamic_cast<ColumnOp*> (fileOps[i]);
        Column curCol;
        // set params
        colOp->initColumn(curCol);
        ColType colType;
        Convertor::convertColType(columns[i].colDataType, colType);
        colOp->setColParam(curCol, 0, columns[i].colWidth,
                           columns[i].colDataType, colType, columns[i].dataFile.oid,
                           columns[i].compressionType,
                           columns[i].dataFile.fDbRoot, columns[i].dataFile.fPartition, columns[i].dataFile.fSegment);
        curCol.dataFile.pFile = columns[i].dataFile.pFile;
        colOp->clearColumn(curCol);
    }

    return rc;
}
#endif
void WriteEngineWrapper::writeVBEnd(const TxnID& txnid, std::vector<BRM::LBIDRange>& rangeList)
{
  if (idbdatafile::IDBPolicy::useHdfs())
    return;

  BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
}

int WriteEngineWrapper::updateColumnRec(const TxnID& txnid, const vector<CSCTypesList>& colExtentsColType,
                                        vector<ColStructList>& colExtentsStruct, ColValueList& colValueList,
                                        vector<void*>& colOldValueList, vector<RIDList>& ridLists,
                                        vector<DctnryStructList>& dctnryExtentsStruct,
                                        DctnryValueList& dctnryValueList, const int32_t tableOid)
{
  int rc = 0;
  unsigned numExtents = colExtentsStruct.size();
  ColStructList colStructList;
  DctnryStructList dctnryStructList;
  WriteEngine::CSCTypesList cscColTypeList;
  ColumnOp* colOp = NULL;
  ExtCPInfoList infosToUpdate;

  if (m_opType != DELETE)
  {
    m_opType = UPDATE;
  }

  for (unsigned extent = 0; extent < numExtents; extent++)
  {
    colStructList = colExtentsStruct[extent];
    dctnryStructList = dctnryExtentsStruct[extent];
    cscColTypeList = colExtentsColType[extent];

    if (m_opType != DELETE)
    {
      // Tokenize data if needed
      vector<Token> tokenList;

      DctColTupleList::iterator dctCol_iter;
      ColTupleList::iterator col_iter;

      for (unsigned i = 0; i < colStructList.size(); i++)
      {
        if (colStructList[i].tokenFlag)
        {
          // only need to tokenize once
          dctCol_iter = dctnryValueList[i].begin();
          Token token;

          if (!dctCol_iter->isNull)
          {
            RETURN_ON_ERROR_REPORT(
                tokenize(txnid, dctnryStructList[i], *dctCol_iter, true));  // @bug 5572 HDFS tmp file
            token = dctCol_iter->token;

#ifdef PROFILE
// timer.stop("tokenize");
#endif
          }

          tokenList.push_back(token);
        }
      }

      int dicPos = 0;

      for (unsigned i = 0; i < colStructList.size(); i++)
      {
        if (colStructList[i].tokenFlag)
        {
          // only need to tokenize once
          col_iter = colValueList[i].begin();

          while (col_iter != colValueList[i].end())
          {
            col_iter->data = tokenList[dicPos];
            col_iter++;
          }

          dicPos++;
        }
      }
    }

    RIDList::iterator rid_iter;
    // Mark extents invalid
    bool successFlag = true;
    unsigned width = 0;
    int curFbo = 0, curBio, lastFbo = -1;
    rid_iter = ridLists[extent].begin();
    RID aRid = *rid_iter;

    ExtCPInfoList currentExtentRanges;
    for (const auto& colStruct : colStructList)
    {
      currentExtentRanges.push_back(
          ExtCPInfo(colStruct.colDataType, colStruct.colWidth));  // temporary for each extent.
    }
    std::vector<ExtCPInfo*> currentExtentRangesPtrs(colStructList.size(), NULL);  // pointers for each extent.

    for (unsigned j = 0; j < colStructList.size(); j++)
    {
      colOp = m_colOp[op(colStructList[j].fCompressionType)];
      ExtCPInfo* cpInfoP = &(currentExtentRanges[j]);
      cpInfoP = getCPInfoToUpdateForUpdatableType(colStructList[j], cpInfoP);
      currentExtentRangesPtrs[j] = cpInfoP;

      if (colStructList[j].tokenFlag)
        continue;

      width = colOp->getCorrectRowWidth(colStructList[j].colDataType, colStructList[j].colWidth);
      successFlag = colOp->calculateRowId(aRid, BYTE_PER_BLOCK / width, width, curFbo, curBio);

      if (successFlag)
      {
        if (curFbo != lastFbo)
        {
          RETURN_ON_ERROR_REPORT(AddLBIDtoList(txnid, colStructList[j], curFbo, cpInfoP));
        }
      }
    }

    //#ifdef PROFILE
    // timer.start("markExtentsInvalid");
    //#endif

    if (m_opType != DELETE)
      m_opType = UPDATE;

    rc = writeColumnRecUpdate(txnid, cscColTypeList, colStructList, colValueList, colOldValueList,
                              ridLists[extent], tableOid, true, ridLists[extent].size(),
                              &currentExtentRangesPtrs);

    if (rc != NO_ERROR)
      break;

    // copy updated ranges into bulk update vector.
    for (auto cpInfoPtr : currentExtentRangesPtrs)
    {
      if (cpInfoPtr)
      {
        cpInfoPtr->fCPInfo.seqNum++;
        infosToUpdate.push_back(*cpInfoPtr);
      }
    }
  }
  markTxnExtentsAsInvalid(txnid);
  if (rc == NO_ERROR)
  {
    ExtCPInfoList infosToDrop = infosToUpdate;
    for (auto& cpInfo : infosToDrop)
    {
      cpInfo.fCPInfo.seqNum = SEQNUM_MARK_INVALID_SET_RANGE;
    }
    rc = BRMWrapper::getInstance()->setExtentsMaxMin(infosToDrop);
    setInvalidCPInfosSpecialMarks(infosToUpdate);
    rc = BRMWrapper::getInstance()->setExtentsMaxMin(infosToUpdate);
  }
  return rc;
}

int WriteEngineWrapper::updateColumnRecs(const TxnID& txnid, const CSCTypesList& cscColTypeList,
                                         vector<ColStruct>& colExtentsStruct, ColValueList& colValueList,
                                         const RIDList& ridLists, const int32_t tableOid)
{
  // Mark extents invalid
  ColumnOp* colOp = NULL;
  bool successFlag = true;
  unsigned width = 0;
  int curFbo = 0, curBio, lastFbo = -1;
  RID aRid = ridLists[0];
  int rc = 0;
  ExtCPInfoList infosToUpdate;
  for (const auto& colStruct : colExtentsStruct)
  {
    infosToUpdate.push_back(ExtCPInfo(colStruct.colDataType, colStruct.colWidth));
  }
  ExtCPInfoList bulkUpdateInfos;
  std::vector<ExtCPInfo*> pointersToInfos;  // pointersToInfos[i] points to infosToUpdate[i] and may be NULL.

  m_opType = UPDATE;

  for (unsigned j = 0; j < colExtentsStruct.size(); j++)
  {
    colOp = m_colOp[op(colExtentsStruct[j].fCompressionType)];

    ExtCPInfo* cpInfoP = &(infosToUpdate[j]);
    cpInfoP = getCPInfoToUpdateForUpdatableType(colExtentsStruct[j], cpInfoP);
    pointersToInfos.push_back(cpInfoP);

    if (colExtentsStruct[j].tokenFlag)
      continue;

    width = colOp->getCorrectRowWidth(colExtentsStruct[j].colDataType, colExtentsStruct[j].colWidth);
    successFlag = colOp->calculateRowId(aRid, BYTE_PER_BLOCK / width, width, curFbo, curBio);

    if (successFlag)
    {
      if (curFbo != lastFbo)
      {
        RETURN_ON_ERROR(AddLBIDtoList(txnid, colExtentsStruct[j], curFbo, cpInfoP));
      }
    }
  }

  markTxnExtentsAsInvalid(txnid);

  if (m_opType != DELETE)
    m_opType = UPDATE;

  for (auto cpInfoP : pointersToInfos)
  {
    if (cpInfoP)
    {
      auto tmp = *cpInfoP;
      tmp.fCPInfo.seqNum = SEQNUM_MARK_INVALID_SET_RANGE;
      bulkUpdateInfos.push_back(tmp);
    }
  }
  if (!bulkUpdateInfos.empty())
  {
    rc = BRMWrapper::getInstance()->setExtentsMaxMin(bulkUpdateInfos);
  }

  rc = writeColumnRecords(txnid, cscColTypeList, colExtentsStruct, colValueList, ridLists, tableOid, true,
                          &pointersToInfos);

  if (rc == NO_ERROR)
  {
    bulkUpdateInfos.clear();
    for (auto cpInfoP : pointersToInfos)
    {
      if (cpInfoP)
      {
        cpInfoP->fCPInfo.seqNum++;
        bulkUpdateInfos.push_back(*cpInfoP);
      }
    }
    if (!bulkUpdateInfos.empty())
    {
      setInvalidCPInfosSpecialMarks(bulkUpdateInfos);
      rc = BRMWrapper::getInstance()->setExtentsMaxMin(bulkUpdateInfos);
    }
  }
  m_opType = NOOP;
  return rc;
}

int WriteEngineWrapper::writeColumnRecords(const TxnID& txnid, const CSCTypesList& cscColTypeList,
                                           vector<ColStruct>& colStructList, ColValueList& colValueList,
                                           const RIDList& ridLists, const int32_t tableOid, bool versioning,
                                           std::vector<ExtCPInfo*>* cpInfos)
{
  bool bExcp;
  int rc = 0;
  void* valArray = NULL;
  void* oldValArray = NULL;
  Column curCol;
  ColStruct curColStruct;
  CalpontSystemCatalog::ColType curColType;
  ColTupleList curTupleList;
  ColStructList::size_type totalColumn;
  ColStructList::size_type i;
  ColTupleList::size_type totalRow;
  setTransId(txnid);
  totalColumn = colStructList.size();
  totalRow = ridLists.size();

  TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);

  for (i = 0; i < totalColumn; i++)
  {
    ExtCPInfo* cpInfo = NULL;
    if (cpInfos)
    {
      cpInfo = (*cpInfos)[i];
    }
    valArray = NULL;
    oldValArray = NULL;

    curColStruct = colStructList[i];
    curColType = cscColTypeList[i];
    curTupleList = colValueList[i];
    ColumnOp* colOp = m_colOp[op(curColStruct.fCompressionType)];

    Convertor::convertColType(&curColStruct);

    // set params
    colOp->initColumn(curCol);

    colOp->setColParam(curCol, 0, curColStruct.colWidth, curColStruct.colDataType, curColStruct.colType,
                       curColStruct.dataOid, curColStruct.fCompressionType, curColStruct.fColDbRoot,
                       curColStruct.fColPartition, curColStruct.fColSegment);
    colOp->findTypeHandler(curColStruct.colWidth, curColStruct.colDataType);
    ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(curColStruct.dataOid);
    ColExtsInfo::iterator it = aColExtsInfo.begin();

    while (it != aColExtsInfo.end())
    {
      if ((it->dbRoot == curColStruct.fColDbRoot) && (it->partNum == curColStruct.fColPartition) &&
          (it->segNum == curColStruct.fColSegment))
        break;

      it++;
    }

    if (it == aColExtsInfo.end())  // add this one to the list
    {
      ColExtInfo aExt;
      aExt.dbRoot = curColStruct.fColDbRoot;
      aExt.partNum = curColStruct.fColPartition;
      aExt.segNum = curColStruct.fColSegment;
      aExt.compType = curColStruct.fCompressionType;
      aExt.isDict = false;
      aColExtsInfo.push_back(aExt);
      aTbaleMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
    }

    string segFile;
    rc = colOp->openColumnFile(curCol, segFile, true);  // @bug 5572 HDFS tmp file

    if (rc != NO_ERROR)
      break;

    vector<LBIDRange> rangeList;

    if (versioning)
    {
      rc = processVersionBuffers(curCol.dataFile.pFile, txnid, curColStruct, curColStruct.colWidth, totalRow,
                                 ridLists, rangeList);
    }

    if (rc != NO_ERROR)
    {
      if (curColStruct.fCompressionType == 0)
      {
        curCol.dataFile.pFile->flush();
      }

      BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
      break;
    }

    allocateValArray(valArray, totalRow, curColStruct.colType, curColStruct.colWidth);

    if (m_opType != INSERT && cpInfo)
    {
      allocateValArray(oldValArray, totalRow, curColStruct.colType, curColStruct.colWidth);
    }

    // convert values to valArray
    bExcp = false;

    try
    {
      convertValArray(totalRow, cscColTypeList[i], curColStruct.colType, curTupleList, valArray);
    }
    catch (...)
    {
      bExcp = true;
    }

    if (bExcp)
    {
      BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
      return ERR_PARSING;
    }

#ifdef PROFILE
    timer.start("writeRow ");
#endif
    rc = colOp->writeRowsValues(curCol, totalRow, ridLists, valArray, oldValArray);
#ifdef PROFILE
    timer.stop("writeRow ");
#endif
    colOp->clearColumn(curCol);

    updateMaxMinRange(totalRow, totalRow, cscColTypeList[i], curColStruct.colType, valArray, oldValArray,
                      cpInfo, false);

    if (curColStruct.fCompressionType == 0)
    {
      std::vector<BRM::FileInfo> files;
      BRM::FileInfo aFile;
      aFile.partitionNum = curColStruct.fColPartition;
      aFile.dbRoot = curColStruct.fColDbRoot;
      ;
      aFile.segmentNum = curColStruct.fColSegment;
      aFile.compType = curColStruct.fCompressionType;
      files.push_back(aFile);

      if (idbdatafile::IDBPolicy::useHdfs())
        cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID());
    }

    BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);

    if (valArray != NULL)
      free(valArray);

    if (oldValArray != NULL)
    {
      free(oldValArray);
    }

    // check error
    if (rc != NO_ERROR)
      break;
  }

  return rc;
}

/*@brief writeColumnRec - Write values to a column
 */
/***********************************************************
 * DESCRIPTION:
 *    Write values to a column
 * PARAMETERS:
 *    tableOid - table object id
 *    cscColTypesList - CSC ColType list
 *    colStructList - column struct list
 *    colValueList - column value list
 *    colNewStructList - the new extent struct list
 *    colNewValueList - column value list for the new extent
 *    rowIdArray -  row id list
 *    useTmpSuffix - use temp suffix for db output file
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in inserting the value
 ***********************************************************/
int WriteEngineWrapper::writeColumnRec(const TxnID& txnid, const CSCTypesList& cscColTypeList,
                                       const ColStructList& colStructList, ColValueList& colValueList,
                                       RID* rowIdArray, const ColStructList& newColStructList,
                                       ColValueList& newColValueList, const int32_t tableOid,
                                       bool useTmpSuffix, bool versioning, ColSplitMaxMinInfoList* maxMins)
{
  bool bExcp;
  int rc = 0;
  void* valArray;
  void* oldValArray;
  string segFile;
  Column curCol;
  ColTupleList oldTupleList;
  ColStructList::size_type totalColumn;
  ColStructList::size_type i;
  ColTupleList::size_type totalRow1, totalRow2;

  setTransId(txnid);

  totalColumn = colStructList.size();
#ifdef PROFILE
  StopWatch timer;
#endif

  totalRow1 = colValueList[0].size();
  totalRow2 = 0;

  if (newColValueList.size() > 0)
    totalRow2 = newColValueList[0].size();

  TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);

  for (i = 0; i < totalColumn; i++)
  {
    if (totalRow2 > 0)
    {
      RID* secondPart = rowIdArray + totalRow1;

      //@Bug 2205 Check if all rows go to the new extent
      if (totalRow1 > 0)
      {
        // Write the first batch
        valArray = NULL;
        oldValArray = NULL;
        RID* firstPart = rowIdArray;
        ColumnOp* colOp = m_colOp[op(colStructList[i].fCompressionType)];

        // set params
        colOp->initColumn(curCol);
        // need to pass real dbRoot, partition, and segment to setColParam
        colOp->setColParam(curCol, 0, colStructList[i].colWidth, colStructList[i].colDataType,
                           colStructList[i].colType, colStructList[i].dataOid,
                           colStructList[i].fCompressionType, colStructList[i].fColDbRoot,
                           colStructList[i].fColPartition, colStructList[i].fColSegment);
        colOp->findTypeHandler(colStructList[i].colWidth, colStructList[i].colDataType);

        ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(colStructList[i].dataOid);
        ColExtsInfo::iterator it = aColExtsInfo.begin();

        while (it != aColExtsInfo.end())
        {
          if ((it->dbRoot == colStructList[i].fColDbRoot) &&
              (it->partNum == colStructList[i].fColPartition) && (it->segNum == colStructList[i].fColSegment))
            break;

          it++;
        }

        if (it == aColExtsInfo.end())  // add this one to the list
        {
          ColExtInfo aExt;
          aExt.dbRoot = colStructList[i].fColDbRoot;
          aExt.partNum = colStructList[i].fColPartition;
          aExt.segNum = colStructList[i].fColSegment;
          aExt.compType = colStructList[i].fCompressionType;
          aColExtsInfo.push_back(aExt);
          aTbaleMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
        }

        rc = colOp->openColumnFile(curCol, segFile, useTmpSuffix, IO_BUFF_SIZE);  // @bug 5572 HDFS tmp file

        if (rc != NO_ERROR)
          break;

        // handling versioning
        vector<LBIDRange> rangeList;

        if (versioning)
        {
          rc = processVersionBuffer(curCol.dataFile.pFile, txnid, colStructList[i], colStructList[i].colWidth,
                                    totalRow1, firstPart, rangeList);

          if (rc != NO_ERROR)
          {
            if (colStructList[i].fCompressionType == 0)
            {
              curCol.dataFile.pFile->flush();
            }

            BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
            break;
          }
        }

        // WIP We can allocate based on column size and not colType
        // have to init the size here
        allocateValArray(valArray, totalRow1, colStructList[i].colType, colStructList[i].colWidth);

        ExtCPInfo* cpInfo = getCPInfoToUpdateForUpdatableType(
            colStructList[i], maxMins ? ((*maxMins)[i]).fSplitMaxMinInfoPtrs[0] : NULL);

        if (m_opType != INSERT && cpInfo != NULL)  // we allocate space for old values only when we need them.
        {
          allocateValArray(oldValArray, totalRow1, colStructList[i].colType, colStructList[i].colWidth);
        }
        else
        {
          oldValArray = NULL;
        }

        // convert values to valArray
        // WIP Is m_opType ever set to DELETE?
        if (m_opType != DELETE)
        {
          bExcp = false;

          try
          {
            // WIP We convert values twice!?
            // dmlcommandproc converts strings to boost::any and this converts
            // into actual type value masked by *void
            // It is not clear why we need to convert to boost::any b/c we can convert from the original
            // string here
            convertValArray(totalRow1, cscColTypeList[i], colStructList[i].colType, colValueList[i],
                            valArray);
          }
          catch (...)
          {
            bExcp = true;
          }

          if (bExcp)
          {
            if (versioning)
              BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);

            return ERR_PARSING;
          }

#ifdef PROFILE
          timer.start("writeRow ");
#endif
          rc = colOp->writeRow(curCol, totalRow1, firstPart, valArray, oldValArray);
#ifdef PROFILE
          timer.stop("writeRow ");
#endif
        }
        else
        {
#ifdef PROFILE
          timer.start("writeRow ");
#endif
          rc = colOp->writeRow(curCol, totalRow1, rowIdArray, valArray, oldValArray, true);
#ifdef PROFILE
          timer.stop("writeRow ");
#endif
        }

        colOp->clearColumn(curCol);

        updateMaxMinRange(totalRow1, totalRow1, cscColTypeList[i], colStructList[i].colType, valArray,
                          oldValArray, cpInfo, rowIdArray[0] == 0 && m_opType == INSERT);

        if (versioning)
          BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);

        if (valArray != NULL)
          free(valArray);

        if (oldValArray != NULL)
          free(oldValArray);

        // check error
        if (rc != NO_ERROR)
          break;
      }

      // Process the second batch
      valArray = NULL;
      oldValArray = NULL;

      ColumnOp* colOp = m_colOp[op(newColStructList[i].fCompressionType)];

      // set params
      colOp->initColumn(curCol);
      colOp->setColParam(curCol, 0, newColStructList[i].colWidth, newColStructList[i].colDataType,
                         newColStructList[i].colType, newColStructList[i].dataOid,
                         newColStructList[i].fCompressionType, newColStructList[i].fColDbRoot,
                         newColStructList[i].fColPartition, newColStructList[i].fColSegment);
      colOp->findTypeHandler(newColStructList[i].colWidth, newColStructList[i].colDataType);

      ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(newColStructList[i].dataOid);
      ColExtsInfo::iterator it = aColExtsInfo.begin();

      while (it != aColExtsInfo.end())
      {
        if ((it->dbRoot == newColStructList[i].fColDbRoot) &&
            (it->partNum == newColStructList[i].fColPartition) &&
            (it->segNum == newColStructList[i].fColSegment))
          break;

        it++;
      }

      if (it == aColExtsInfo.end())  // add this one to the list
      {
        ColExtInfo aExt;
        aExt.dbRoot = newColStructList[i].fColDbRoot;
        aExt.partNum = newColStructList[i].fColPartition;
        aExt.segNum = newColStructList[i].fColSegment;
        aExt.compType = newColStructList[i].fCompressionType;
        aColExtsInfo.push_back(aExt);
        aTbaleMetaData->setColExtsInfo(newColStructList[i].dataOid, aColExtsInfo);
      }

      // Pass "false" for hdfs tmp file flag.  Since we only allow 1
      // extent per segment file (with HDFS), we can assume a second
      // extent is going to a new file (and won't need tmp file).
      rc = colOp->openColumnFile(curCol, segFile, false, IO_BUFF_SIZE);  // @bug 5572 HDFS tmp file

      if (rc != NO_ERROR)
        break;

      // handling versioning
      vector<LBIDRange> rangeList;

      if (versioning)
      {
        rc = processVersionBuffer(curCol.dataFile.pFile, txnid, newColStructList[i],
                                  newColStructList[i].colWidth, totalRow2, secondPart, rangeList);

        if (rc != NO_ERROR)
        {
          if (newColStructList[i].fCompressionType == 0)
          {
            curCol.dataFile.pFile->flush();
          }

          BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
          break;
        }
      }

      ExtCPInfo* cpInfo = getCPInfoToUpdateForUpdatableType(
          newColStructList[i], maxMins ? ((*maxMins)[i]).fSplitMaxMinInfoPtrs[1] : NULL);
      allocateValArray(valArray, totalRow2, newColStructList[i].colType, newColStructList[i].colWidth);

      if (m_opType != INSERT && cpInfo != NULL)  // we allocate space for old values only when we need them.
      {
        allocateValArray(oldValArray, totalRow1, colStructList[i].colType, colStructList[i].colWidth);
      }
      else
      {
        oldValArray = NULL;
      }

      // convert values to valArray
      if (m_opType != DELETE)
      {
        bExcp = false;

        try
        {
          convertValArray(totalRow2, cscColTypeList[i], newColStructList[i].colType, newColValueList[i],
                          valArray);
        }
        catch (...)
        {
          bExcp = true;
        }

        if (bExcp)
        {
          if (versioning)
            BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);

          return ERR_PARSING;
        }

#ifdef PROFILE
        timer.start("writeRow ");
#endif
        rc = colOp->writeRow(
            curCol, totalRow2, secondPart, valArray,
            oldValArray);  // XXX: here we use secondPart array and just below we use rowIdArray. WHY???
#ifdef PROFILE
        timer.stop("writeRow ");
#endif
      }
      else
      {
#ifdef PROFILE
        timer.start("writeRow ");
#endif
        rc = colOp->writeRow(
            curCol, totalRow2, rowIdArray, valArray, oldValArray,
            true);  // XXX: BUG: here we use rowIdArray and just above we use secondPart array. WHY???
#ifdef PROFILE
        timer.stop("writeRow ");
#endif
      }

      colOp->clearColumn(curCol);

      updateMaxMinRange(totalRow2, totalRow2, cscColTypeList[i], newColStructList[i].colType, valArray,
                        oldValArray, cpInfo, secondPart[0] == 0 && m_opType == INSERT);

      if (versioning)
        BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);

      if (valArray != NULL)
        free(valArray);

      // check error
      if (rc != NO_ERROR)
        break;
    }
    else
    {
      valArray = NULL;
      oldValArray = NULL;

      ColumnOp* colOp = m_colOp[op(colStructList[i].fCompressionType)];

      ExtCPInfo* cpInfo = getCPInfoToUpdateForUpdatableType(
          colStructList[i], maxMins ? ((*maxMins)[i]).fSplitMaxMinInfoPtrs[0] : NULL);

      // set params
      colOp->initColumn(curCol);
      colOp->setColParam(curCol, 0, colStructList[i].colWidth, colStructList[i].colDataType,
                         colStructList[i].colType, colStructList[i].dataOid,
                         colStructList[i].fCompressionType, colStructList[i].fColDbRoot,
                         colStructList[i].fColPartition, colStructList[i].fColSegment);
      colOp->findTypeHandler(colStructList[i].colWidth, colStructList[i].colDataType);

      rc = colOp->openColumnFile(curCol, segFile, useTmpSuffix, IO_BUFF_SIZE);  // @bug 5572 HDFS tmp file

      // cout << " Opened file oid " << curCol.dataFile.pFile << endl;
      if (rc != NO_ERROR)
        break;

      ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(colStructList[i].dataOid);
      ColExtsInfo::iterator it = aColExtsInfo.begin();

      while (it != aColExtsInfo.end())
      {
        if ((it->dbRoot == colStructList[i].fColDbRoot) && (it->partNum == colStructList[i].fColPartition) &&
            (it->segNum == colStructList[i].fColSegment))
          break;

        it++;
      }

      if (it == aColExtsInfo.end())  // add this one to the list
      {
        ColExtInfo aExt;
        aExt.dbRoot = colStructList[i].fColDbRoot;
        aExt.partNum = colStructList[i].fColPartition;
        aExt.segNum = colStructList[i].fColSegment;
        aExt.compType = colStructList[i].fCompressionType;
        aColExtsInfo.push_back(aExt);
        aTbaleMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
      }

      // handling versioning
      vector<LBIDRange> rangeList;

      if (versioning)
      {
        rc = processVersionBuffer(curCol.dataFile.pFile, txnid, colStructList[i], colStructList[i].colWidth,
                                  totalRow1, rowIdArray, rangeList);

        if (rc != NO_ERROR)
        {
          if (colStructList[i].fCompressionType == 0)
          {
            curCol.dataFile.pFile->flush();
          }

          BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
          break;
        }
      }

      allocateValArray(valArray, totalRow1, colStructList[i].colType, colStructList[i].colWidth);

      if (m_opType != INSERT && cpInfo != NULL)  // we allocate space for old values only when we need them.
      {
        allocateValArray(oldValArray, totalRow1, colStructList[i].colType, colStructList[i].colWidth);
      }
      else
      {
        oldValArray = NULL;
      }

      // convert values to valArray
      if (m_opType != DELETE)
      {
        bExcp = false;

        try
        {
          convertValArray(totalRow1, cscColTypeList[i], colStructList[i].colType, colValueList[i], valArray,
                          true);
        }
        catch (...)
        {
          bExcp = true;
        }

        if (bExcp)
        {
          if (versioning)
            BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);

          return ERR_PARSING;
        }

#ifdef PROFILE
        timer.start("writeRow ");
#endif
        rc = colOp->writeRow(curCol, totalRow1, rowIdArray, valArray, oldValArray);
#ifdef PROFILE
        timer.stop("writeRow ");
#endif
      }
      else
      {
#ifdef PROFILE
        timer.start("writeRow ");
#endif
        rc = colOp->writeRow(curCol, totalRow1, rowIdArray, valArray, oldValArray, true);
#ifdef PROFILE
        timer.stop("writeRow ");
#endif
      }

      colOp->clearColumn(curCol);

      updateMaxMinRange(totalRow1, totalRow1, cscColTypeList[i], colStructList[i].colType, valArray,
                        oldValArray, cpInfo, rowIdArray[0] == 0 && m_opType == INSERT);

      if (versioning)
        BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);

      if (valArray != NULL)
        free(valArray);

      if (oldValArray != NULL)
        free(oldValArray);

      // check error
      if (rc != NO_ERROR)
        break;
    }
  }  // end of for (i = 0

#ifdef PROFILE
  timer.finish();
#endif
  return rc;
}

int WriteEngineWrapper::writeColumnRecBinary(const TxnID& txnid, const ColStructList& colStructList,
                                             std::vector<uint64_t>& colValueList, RID* rowIdArray,
                                             const ColStructList& newColStructList,
                                             std::vector<uint64_t>& newColValueList, const int32_t tableOid,
                                             bool useTmpSuffix, bool versioning)
{
  int rc = 0;
  void* valArray = NULL;
  string segFile;
  Column curCol;
  ColStructList::size_type totalColumn;
  ColStructList::size_type i;
  size_t totalRow1, totalRow2;

  setTransId(txnid);

  totalColumn = colStructList.size();
#ifdef PROFILE
  StopWatch timer;
#endif

  totalRow1 = colValueList.size() / totalColumn;

  if (newColValueList.size() > 0)
  {
    totalRow2 = newColValueList.size() / newColStructList.size();
    totalRow1 -= totalRow2;
  }
  else
  {
    totalRow2 = 0;
  }

  // It is possible totalRow1 is zero but totalRow2 has values
  if ((totalRow1 == 0) && (totalRow2 == 0))
    return rc;

  TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);

  if (totalRow1)
  {
    valArray = malloc(sizeof(uint64_t) * totalRow1);

    for (i = 0; i < totalColumn; i++)
    {
      //@Bug 2205 Check if all rows go to the new extent
      // Write the first batch
      RID* firstPart = rowIdArray;
      ColumnOp* colOp = m_colOp[op(colStructList[i].fCompressionType)];

      // set params
      colOp->initColumn(curCol);
      // need to pass real dbRoot, partition, and segment to setColParam
      colOp->setColParam(curCol, 0, colStructList[i].colWidth, colStructList[i].colDataType,
                         colStructList[i].colType, colStructList[i].dataOid,
                         colStructList[i].fCompressionType, colStructList[i].fColDbRoot,
                         colStructList[i].fColPartition, colStructList[i].fColSegment);
      colOp->findTypeHandler(colStructList[i].colWidth, colStructList[i].colDataType);

      ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(colStructList[i].dataOid);
      ColExtsInfo::iterator it = aColExtsInfo.begin();

      while (it != aColExtsInfo.end())
      {
        if ((it->dbRoot == colStructList[i].fColDbRoot) && (it->partNum == colStructList[i].fColPartition) &&
            (it->segNum == colStructList[i].fColSegment))
          break;

        it++;
      }

      if (it == aColExtsInfo.end())  // add this one to the list
      {
        ColExtInfo aExt;
        aExt.dbRoot = colStructList[i].fColDbRoot;
        aExt.partNum = colStructList[i].fColPartition;
        aExt.segNum = colStructList[i].fColSegment;
        aExt.compType = colStructList[i].fCompressionType;
        aColExtsInfo.push_back(aExt);
        aTbaleMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
      }

      rc = colOp->openColumnFile(curCol, segFile, useTmpSuffix, IO_BUFF_SIZE);  // @bug 5572 HDFS tmp file

      if (rc != NO_ERROR)
        break;

      // handling versioning
      vector<LBIDRange> rangeList;

      if (versioning)
      {
        rc = processVersionBuffer(curCol.dataFile.pFile, txnid, colStructList[i], colStructList[i].colWidth,
                                  totalRow1, firstPart, rangeList);

        if (rc != NO_ERROR)
        {
          if (colStructList[i].fCompressionType == 0)
          {
            curCol.dataFile.pFile->flush();
          }

          BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
          break;
        }
      }

      // totalRow1 -= totalRow2;
      // have to init the size here
      // nullArray = (bool*) malloc(sizeof(bool) * totalRow);
      uint8_t tmp8;
      uint16_t tmp16;
      uint32_t tmp32;

      for (size_t j = 0; j < totalRow1; j++)
      {
        uint64_t curValue = colValueList[((totalRow1 + totalRow2) * i) + j];

        switch (colStructList[i].colType)
        {
          case WriteEngine::WR_VARBINARY:  // treat same as char for now
          case WriteEngine::WR_CHAR:
          case WriteEngine::WR_BLOB:
          case WriteEngine::WR_TEXT: ((uint64_t*)valArray)[j] = curValue; break;

          case WriteEngine::WR_INT:
          case WriteEngine::WR_UINT:
          case WriteEngine::WR_MEDINT:
          case WriteEngine::WR_UMEDINT:
          case WriteEngine::WR_FLOAT:
            tmp32 = curValue;
            ((uint32_t*)valArray)[j] = tmp32;
            break;

          case WriteEngine::WR_ULONGLONG:
          case WriteEngine::WR_LONGLONG:
          case WriteEngine::WR_DOUBLE:
          case WriteEngine::WR_TOKEN: ((uint64_t*)valArray)[j] = curValue; break;

          case WriteEngine::WR_BYTE:
          case WriteEngine::WR_UBYTE:
            tmp8 = curValue;
            ((uint8_t*)valArray)[j] = tmp8;
            break;

          case WriteEngine::WR_SHORT:
          case WriteEngine::WR_USHORT:
            tmp16 = curValue;
            ((uint16_t*)valArray)[j] = tmp16;
            break;

          case WriteEngine::WR_BINARY: ((uint64_t*)valArray)[j] = curValue; break;
        }
      }

#ifdef PROFILE
      timer.start("writeRow ");
#endif
      rc = colOp->writeRow(curCol, totalRow1, firstPart, valArray);
#ifdef PROFILE
      timer.stop("writeRow ");
#endif
      colOp->closeColumnFile(curCol);

      if (versioning)
        BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);

      // check error
      if (rc != NO_ERROR)
        break;

    }  // end of for (i = 0

    if (valArray != NULL)
    {
      free(valArray);
      valArray = NULL;
    }
  }

  // MCOL-1176 - Write second extent
  if (totalRow2)
  {
    valArray = malloc(sizeof(uint64_t) * totalRow2);

    for (i = 0; i < newColStructList.size(); i++)
    {
      //@Bug 2205 Check if all rows go to the new extent
      // Write the first batch
      RID* secondPart = rowIdArray + totalRow1;
      ColumnOp* colOp = m_colOp[op(newColStructList[i].fCompressionType)];

      // set params
      colOp->initColumn(curCol);
      // need to pass real dbRoot, partition, and segment to setColParam
      colOp->setColParam(curCol, 0, newColStructList[i].colWidth, newColStructList[i].colDataType,
                         newColStructList[i].colType, newColStructList[i].dataOid,
                         newColStructList[i].fCompressionType, newColStructList[i].fColDbRoot,
                         newColStructList[i].fColPartition, newColStructList[i].fColSegment);
      colOp->findTypeHandler(newColStructList[i].colWidth, newColStructList[i].colDataType);

      ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(newColStructList[i].dataOid);
      ColExtsInfo::iterator it = aColExtsInfo.begin();

      while (it != aColExtsInfo.end())
      {
        if ((it->dbRoot == newColStructList[i].fColDbRoot) &&
            (it->partNum == newColStructList[i].fColPartition) &&
            (it->segNum == colStructList[i].fColSegment))
          break;

        it++;
      }

      if (it == aColExtsInfo.end())  // add this one to the list
      {
        ColExtInfo aExt;
        aExt.dbRoot = newColStructList[i].fColDbRoot;
        aExt.partNum = newColStructList[i].fColPartition;
        aExt.segNum = newColStructList[i].fColSegment;
        aExt.compType = newColStructList[i].fCompressionType;
        aColExtsInfo.push_back(aExt);
        aTbaleMetaData->setColExtsInfo(newColStructList[i].dataOid, aColExtsInfo);
      }

      rc = colOp->openColumnFile(curCol, segFile, useTmpSuffix, IO_BUFF_SIZE);  // @bug 5572 HDFS tmp file

      if (rc != NO_ERROR)
        break;

      // handling versioning
      vector<LBIDRange> rangeList;

      if (versioning)
      {
        rc = processVersionBuffer(curCol.dataFile.pFile, txnid, newColStructList[i],
                                  newColStructList[i].colWidth, totalRow2, secondPart, rangeList);

        if (rc != NO_ERROR)
        {
          if (newColStructList[i].fCompressionType == 0)
          {
            curCol.dataFile.pFile->flush();
          }

          BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
          break;
        }
      }

      // totalRow1 -= totalRow2;
      // have to init the size here
      // nullArray = (bool*) malloc(sizeof(bool) * totalRow);
      uint8_t tmp8;
      uint16_t tmp16;
      uint32_t tmp32;

      for (size_t j = 0; j < totalRow2; j++)
      {
        uint64_t curValue = newColValueList[(totalRow2 * i) + j];

        switch (newColStructList[i].colType)
        {
          case WriteEngine::WR_VARBINARY:  // treat same as char for now
          case WriteEngine::WR_CHAR:
          case WriteEngine::WR_BLOB:
          case WriteEngine::WR_TEXT: ((uint64_t*)valArray)[j] = curValue; break;

          case WriteEngine::WR_INT:
          case WriteEngine::WR_UINT:
          case WriteEngine::WR_MEDINT:
          case WriteEngine::WR_UMEDINT:
          case WriteEngine::WR_FLOAT:
            tmp32 = curValue;
            ((uint32_t*)valArray)[j] = tmp32;
            break;

          case WriteEngine::WR_ULONGLONG:
          case WriteEngine::WR_LONGLONG:
          case WriteEngine::WR_DOUBLE:
          case WriteEngine::WR_TOKEN: ((uint64_t*)valArray)[j] = curValue; break;

          case WriteEngine::WR_BYTE:
          case WriteEngine::WR_UBYTE:
            tmp8 = curValue;
            ((uint8_t*)valArray)[j] = tmp8;
            break;

          case WriteEngine::WR_SHORT:
          case WriteEngine::WR_USHORT:
            tmp16 = curValue;
            ((uint16_t*)valArray)[j] = tmp16;
            break;

          case WriteEngine::WR_BINARY: ((uint64_t*)valArray)[j] = curValue; break;
        }
      }

#ifdef PROFILE
      timer.start("writeRow ");
#endif
      rc = colOp->writeRow(curCol, totalRow2, secondPart, valArray);
#ifdef PROFILE
      timer.stop("writeRow ");
#endif
      colOp->closeColumnFile(curCol);

      if (versioning)
        BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);

      // check error
      if (rc != NO_ERROR)
        break;

    }  // end of for (i = 0
  }

  if (valArray != NULL)
    free(valArray);

#ifdef PROFILE
  timer.finish();
#endif
  return rc;
}

int WriteEngineWrapper::writeColumnRecUpdate(const TxnID& txnid, const CSCTypesList& cscColTypeList,
                                             const ColStructList& colStructList,
                                             const ColValueList& colValueList, vector<void*>& colOldValueList,
                                             const RIDList& ridList, const int32_t tableOid,
                                             bool convertStructFlag, ColTupleList::size_type nRows,
                                             std::vector<ExtCPInfo*>* cpInfos)
{
  bool bExcp;
  int rc = 0;
  void* valArray = NULL;
  void* oldValArray = NULL;
  Column curCol;
  ColStruct curColStruct;
  ColTupleList curTupleList, oldTupleList;
  ColStructList::size_type totalColumn;
  ColStructList::size_type i;
  ColTupleList::size_type totalRow;

  setTransId(txnid);
  colOldValueList.clear();
  totalColumn = colStructList.size();
  totalRow = nRows;

#ifdef PROFILE
  StopWatch timer;
#endif

  vector<LBIDRange> rangeListTot;
  std::vector<VBRange> freeList;
  vector<vector<uint32_t> > fboLists;
  vector<vector<LBIDRange> > rangeLists;
  rc = processBeginVBCopy(txnid, colStructList, ridList, freeList, fboLists, rangeLists, rangeListTot);

  if (rc != NO_ERROR)
  {
    if (rangeListTot.size() > 0)
      BRMWrapper::getInstance()->writeVBEnd(txnid, rangeListTot);

    switch (rc)
    {
      case BRM::ERR_DEADLOCK: return ERR_BRM_DEAD_LOCK;

      case BRM::ERR_VBBM_OVERFLOW: return ERR_BRM_VB_OVERFLOW;

      case BRM::ERR_NETWORK: return ERR_BRM_NETWORK;

      case BRM::ERR_READONLY: return ERR_BRM_READONLY;

      default: return ERR_BRM_BEGIN_COPY;
    }
  }

  VBRange aRange;
  uint32_t blocksProcessedThisOid = 0;
  uint32_t blocksProcessed = 0;
  std::vector<BRM::FileInfo> files;
  TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);

  for (i = 0; i < totalColumn; i++)
  {
    valArray = NULL;
    oldValArray = NULL;
    curColStruct = colStructList[i];
    curTupleList = colValueList[i];  // same value for all rows
    ColumnOp* colOp = m_colOp[op(curColStruct.fCompressionType)];

    // convert column data type
    if (convertStructFlag)
      Convertor::convertColType(&curColStruct);

    // set params
    colOp->initColumn(curCol);
    colOp->setColParam(curCol, 0, curColStruct.colWidth, curColStruct.colDataType, curColStruct.colType,
                       curColStruct.dataOid, curColStruct.fCompressionType, curColStruct.fColDbRoot,
                       curColStruct.fColPartition, curColStruct.fColSegment);
    colOp->findTypeHandler(curColStruct.colWidth, curColStruct.colDataType);

    ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(curColStruct.dataOid);
    ColExtsInfo::iterator it = aColExtsInfo.begin();

    while (it != aColExtsInfo.end())
    {
      if ((it->dbRoot == curColStruct.fColDbRoot) && (it->partNum == curColStruct.fColPartition) &&
          (it->segNum == curColStruct.fColSegment))
        break;

      it++;
    }

    if (it == aColExtsInfo.end())  // add this one to the list
    {
      ColExtInfo aExt;
      aExt.dbRoot = curColStruct.fColDbRoot;
      aExt.partNum = curColStruct.fColPartition;
      aExt.segNum = curColStruct.fColSegment;
      aExt.compType = curColStruct.fCompressionType;
      aColExtsInfo.push_back(aExt);
      aTbaleMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
    }

    string segFile;
    rc = colOp->openColumnFile(curCol, segFile, true, IO_BUFF_SIZE);  // @bug 5572 HDFS tmp file

    if (rc != NO_ERROR)
      break;

    if (curColStruct.fCompressionType == 0)
    {
      BRM::FileInfo aFile;
      aFile.oid = curColStruct.dataOid;
      aFile.partitionNum = curColStruct.fColPartition;
      aFile.dbRoot = curColStruct.fColDbRoot;
      ;
      aFile.segmentNum = curColStruct.fColSegment;
      aFile.compType = curColStruct.fCompressionType;
      files.push_back(aFile);
    }

    // handling versioning
    std::vector<VBRange> curFreeList;
    uint32_t blockUsed = 0;

    if (!idbdatafile::IDBPolicy::useHdfs())
    {
      if (rangeListTot.size() > 0)
      {
        if (freeList[0].size >= (blocksProcessed + rangeLists[i].size()))
        {
          aRange.vbOID = freeList[0].vbOID;
          aRange.vbFBO = freeList[0].vbFBO + blocksProcessed;
          aRange.size = rangeLists[i].size();
          curFreeList.push_back(aRange);
        }
        else
        {
          aRange.vbOID = freeList[0].vbOID;
          aRange.vbFBO = freeList[0].vbFBO + blocksProcessed;
          aRange.size = freeList[0].size - blocksProcessed;
          blockUsed = aRange.size;
          curFreeList.push_back(aRange);

          if (freeList.size() > 1)
          {
            aRange.vbOID = freeList[1].vbOID;
            aRange.vbFBO = freeList[1].vbFBO + blocksProcessedThisOid;
            aRange.size = rangeLists[i].size() - blockUsed;
            curFreeList.push_back(aRange);
            blocksProcessedThisOid += aRange.size;
          }
          else
          {
            rc = 1;
            break;
          }
        }

        blocksProcessed += rangeLists[i].size();

        rc = BRMWrapper::getInstance()->writeVB(curCol.dataFile.pFile, (BRM::VER_t)txnid,
                                                curColStruct.dataOid, fboLists[i], rangeLists[i], colOp,
                                                curFreeList, curColStruct.fColDbRoot, true);
      }
    }

    if (rc != NO_ERROR)
    {
      if (curColStruct.fCompressionType == 0)
      {
        curCol.dataFile.pFile->flush();
      }

      if (rangeListTot.size() > 0)
        BRMWrapper::getInstance()->writeVBEnd(txnid, rangeListTot);

      break;
    }

    allocateValArray(valArray, 1, curColStruct.colType, curColStruct.colWidth);

    ExtCPInfo* cpInfo;
    cpInfo = cpInfos ? ((*cpInfos)[i]) : NULL;

    if (cpInfo)
    {
      allocateValArray(oldValArray, totalRow, curColStruct.colType, curColStruct.colWidth);
    }

    // convert values to valArray
    if (m_opType != DELETE)
    {
      bExcp = false;
      ColTuple curTuple;
      curTuple = curTupleList[0];

      try
      {
        convertValue(cscColTypeList[i], curColStruct.colType, valArray, curTuple.data);
      }
      catch (...)
      {
        bExcp = true;
      }

      if (bExcp)
      {
        if (rangeListTot.size() > 0)
          BRMWrapper::getInstance()->writeVBEnd(txnid, rangeListTot);

        return ERR_PARSING;
      }

#ifdef PROFILE
      timer.start("writeRow ");
#endif
      rc = colOp->writeRows(curCol, totalRow, ridList, valArray, oldValArray);
#ifdef PROFILE
      timer.stop("writeRow ");
#endif
    }
    else
    {
#ifdef PROFILE
      timer.start("writeRows ");
#endif
      rc = colOp->writeRows(curCol, totalRow, ridList, valArray, oldValArray, true);
#ifdef PROFILE
      timer.stop("writeRows ");
#endif
    }

    updateMaxMinRange(1, totalRow, cscColTypeList[i], curColStruct.colType,
                      m_opType == DELETE ? NULL : valArray, oldValArray, cpInfo, false);
    // timer.start("Delete:closefile");
    colOp->clearColumn(curCol);

    // timer.stop("Delete:closefile");
    if (valArray != NULL)
    {
      free(valArray);
      valArray = NULL;
    }

    if (oldValArray != NULL)
    {
      free(oldValArray);
      oldValArray = NULL;
    }

    // check error
    if (rc != NO_ERROR)
      break;

  }  // end of for (i = 0)

  // timer.start("Delete:purgePrimProcFdCache");
  if ((idbdatafile::IDBPolicy::useHdfs()) && (files.size() > 0))
    cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID());

  // timer.stop("Delete:purgePrimProcFdCache");
  if (rangeListTot.size() > 0)
    BRMWrapper::getInstance()->writeVBEnd(txnid, rangeListTot);

  // timer.stop("Delete:writecolrec");
  //#ifdef PROFILE
  // timer.finish();
  //#endif
  return rc;
}

/*@brief tokenize - return a token for a given signature and size
 */
/***********************************************************
 * DESCRIPTION:
 *  return a token for a given signature and size
 *  If it is not in the dictionary, the signature
 *  will be added to the dictionary and the index tree
 *  If it is already in the dictionary, then
 *  the token will be returned
 *  This function does not open and close files.
 *  users need to use openDctnry and CloseDctnry
 * PARAMETERS:
 *  DctnryTuple& dctnryTuple - holds the sigValue, sigSize and token
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in inserting the value
 ***********************************************************/
int WriteEngineWrapper::tokenize(const TxnID& txnid, DctnryTuple& dctnryTuple, int ct)
{
  int cop = op(ct);
  m_dctnry[cop]->setTransId(txnid);
  return m_dctnry[cop]->updateDctnry(dctnryTuple.sigValue, dctnryTuple.sigSize, dctnryTuple.token);
}

/*@brief tokenize - return a token for a given signature and size
 *                          accept OIDs as input
 */
/***********************************************************
 * DESCRIPTION:
 *  Token for a given signature and size
 *  If it is not in the dictionary, the signature
 *  will be added to the dictionary and the index tree
 *  If it is already in the dictionary, then
 *  the token will be returned
 * PARAMETERS:
 *  DctnryTuple& dctnryTuple - holds the sigValue, sigSize and token
 *  DctnryStruct dctnryStruct- contain the 3 OID for dictionary,
 *                             tree and list.
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in inserting the value
 ***********************************************************/
int WriteEngineWrapper::tokenize(const TxnID& txnid, DctnryStruct& dctnryStruct, DctnryTuple& dctnryTuple,
                                 bool useTmpSuffix)  // @bug 5572 HDFS tmp file
{
  // find the corresponding column segment file the token is going to be inserted.

  Dctnry* dctnry = m_dctnry[op(dctnryStruct.fCompressionType)];
  int rc = dctnry->openDctnry(dctnryStruct.dctnryOid, dctnryStruct.fColDbRoot, dctnryStruct.fColPartition,
                              dctnryStruct.fColSegment,
                              useTmpSuffix);  // @bug 5572 TBD

  if (rc != NO_ERROR)
    return rc;

  rc = tokenize(txnid, dctnryTuple, dctnryStruct.fCompressionType);

  int rc2 = dctnry->closeDctnry(true);  // close file, even if tokenize() fails

  if ((rc == NO_ERROR) && (rc2 != NO_ERROR))
    rc = rc2;

  return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Create column files, including data and bitmap files
 * PARAMETERS:
 *    dataOid - column data file id
 *    bitmapOid - column bitmap file id
 *    colWidth - column width
 *    dbRoot   - DBRoot where file is to be located
 *    partition - Starting partition number for segment file path
 *     segment - segment number
 *     compressionType - compression type
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_EXIST if file exists
 *    ERR_FILE_CREATE if something wrong in creating the file
 ***********************************************************/
int WriteEngineWrapper::createDctnry(const TxnID& txnid, const OID& dctnryOid, int colWidth, uint16_t dbRoot,
                                     uint32_t partiotion, uint16_t segment, int compressionType)
{
  BRM::LBID_t startLbid;
  return m_dctnry[op(compressionType)]->createDctnry(dctnryOid, colWidth, dbRoot, partiotion, segment,
                                                     startLbid);
}

int WriteEngineWrapper::convertRidToColumn(RID& rid, uint16_t& dbRoot, uint32_t& partition, uint16_t& segment,
                                           RID filesPerColumnPartition, RID extentsPerSegmentFile,
                                           RID extentRows, uint16_t startDBRoot, unsigned dbrootCnt)
{
  int rc = 0;
  partition = rid / (filesPerColumnPartition * extentsPerSegmentFile * extentRows);

  segment = (((rid % (filesPerColumnPartition * extentsPerSegmentFile * extentRows)) / extentRows)) %
            filesPerColumnPartition;

  dbRoot = ((startDBRoot - 1 + segment) % dbrootCnt) + 1;

  // Calculate the relative rid for this segment file
  RID relRidInPartition =
      rid - ((RID)partition * (RID)filesPerColumnPartition * (RID)extentsPerSegmentFile * (RID)extentRows);
  assert(relRidInPartition <= (RID)filesPerColumnPartition * (RID)extentsPerSegmentFile * (RID)extentRows);
  uint32_t numExtentsInThisPart = relRidInPartition / extentRows;
  unsigned numExtentsInThisSegPart = numExtentsInThisPart / filesPerColumnPartition;
  RID relRidInThisExtent = relRidInPartition - numExtentsInThisPart * extentRows;
  rid = relRidInThisExtent + numExtentsInThisSegPart * extentRows;
  return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Clears table lock for the specified table lock ID.
 * PARAMETERS:
 *    lockID - table lock to be released
 *    errMsg - if error occurs, this is the return error message
 * RETURN:
 *    NO_ERROR if operation is successful
 ***********************************************************/
int WriteEngineWrapper::clearTableLockOnly(uint64_t lockID, std::string& errMsg)
{
  bool bReleased;

  int rc = BRMWrapper::getInstance()->releaseTableLock(lockID, bReleased, errMsg);

  return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Rolls back the state of the extentmap and database files for the
 *    specified table OID, using the metadata previously saved to disk.
 *    Also clears the table lock for the specified table OID.
 * PARAMETERS:
 *    tableOid - table OID to be rolled back
 *    lockID   - table lock corresponding to tableOid
 *    tableName - table name associated with tableOid
 *    applName - application that is driving this bulk rollback
 *    debugConsole - enable debug logging to the console
 *    errorMsg - error message explaining any rollback failure
 * RETURN:
 *    NO_ERROR if rollback completed succesfully
 ***********************************************************/
int WriteEngineWrapper::bulkRollback(OID tableOid, uint64_t lockID, const std::string& tableName,
                                     const std::string& applName, bool debugConsole, string& errorMsg)
{
  errorMsg.clear();

  BulkRollbackMgr rollbackMgr(tableOid, lockID, tableName, applName);

  if (debugConsole)
    rollbackMgr.setDebugConsole(true);

  // We used to pass "false" to not keep (delete) the metafiles at the end of
  // the rollback.  But after the transition to sharedNothing, we pass "true"
  // to initially keep these files.  The metafiles are deleted later, only
  // after all the distributed bulk rollbacks are successfully completed.
  int rc = rollbackMgr.rollback(true);

  if (rc != NO_ERROR)
    errorMsg = rollbackMgr.getErrorMsg();

  // Ignore the return code for now; more important to base rc on the
  // success or failure of the previous work
  BRMWrapper::getInstance()->takeSnapshot();

  return rc;
}

int WriteEngineWrapper::rollbackCommon(const TxnID& txnid, int sessionId)
{
  // Remove the unwanted tmp files and recover compressed chunks.
  string prefix;

  // BUG 4312
  RemoveTxnFromLBIDMap(txnid);
  RemoveTxnFromDictMap(txnid);

  config::Config* config = config::Config::makeConfig();
  prefix = config->getConfig("SystemConfig", "DBRMRoot");

  if (prefix.length() == 0)
  {
    cerr << "Need a valid DBRMRoot entry in Calpont configuation file";
    return ERR_INVALID_PARAM;
  }

  uint64_t pos = prefix.find_last_of("/");
  std::string aDMLLogFileName;

  if (pos != string::npos)
  {
    aDMLLogFileName = prefix.substr(0, pos + 1);  // Get the file path
  }
  else
  {
    logging::Message::Args args;
    args.add("RollbackTran cannot find the dbrm directory for the DML log file");
    SimpleSysLog::instance()->logMsg(args, logging::LOG_TYPE_CRITICAL, logging::M0007);
    return ERR_OPEN_DML_LOG;
  }

  std::ostringstream oss;
  oss << txnid << "_" << Config::getLocalModuleID();
  aDMLLogFileName += "DMLLog_" + oss.str();

  if (IDBPolicy::exists(aDMLLogFileName.c_str()))
  {
    // TODO-for now the DML log file will always be in a local
    // filesystem since IDBDataFile doesn't have any support for
    // a cpp iostream interface.  need to decide if this is ok.
    boost::scoped_ptr<IDBDataFile> aDMLLogFile(IDBDataFile::open(
        IDBPolicy::getType(aDMLLogFileName.c_str(), IDBPolicy::WRITEENG), aDMLLogFileName.c_str(), "r", 0));

    if (aDMLLogFile)  // need recover
    {
      ssize_t fileSize = aDMLLogFile->size();
      boost::scoped_array<char> buf(new char[fileSize]);

      if (aDMLLogFile->read(buf.get(), fileSize) != fileSize)
        return ERR_FILE_READ;

      std::istringstream strstream(string(buf.get(), fileSize));
      std::string backUpFileType;
      std::string filename;
      int64_t size;
      int64_t offset;

      while (strstream >> backUpFileType >> filename >> size >> offset)
      {
        // cout << "Found: " <<  backUpFileType << " name " << filename << "size: " << size << " offset: " <<
        // offset << endl;
        std::ostringstream oss;
        oss << "RollbackTran found " << backUpFileType << " name " << filename << " size: " << size
            << " offset: " << offset;
        logging::Message::Args args;
        args.add(oss.str());
        SimpleSysLog::instance()->logMsg(args, logging::LOG_TYPE_INFO, logging::M0007);

        if (backUpFileType.compare("rlc") == 0)
        {
          // remove the rlc file
          filename += ".rlc";
          // cout << " File removed: " << filename << endl;
          IDBPolicy::remove(filename.c_str());
          logging::Message::Args args1;
          args1.add(filename);
          args1.add(" is removed.");
          SimpleSysLog::instance()->logMsg(args1, logging::LOG_TYPE_INFO, logging::M0007);
        }
        else if (backUpFileType.compare("tmp") == 0)
        {
          int rc = NO_ERROR;
          string orig(filename + ".orig");

          // restore the orig file
          if (IDBPolicy::exists(orig.c_str()))
          {
            // not likely both cdf and tmp exist
            if (IDBPolicy::exists(filename.c_str()) && IDBPolicy::remove(filename.c_str()) != 0)
              rc = ERR_COMP_REMOVE_FILE;

            if (rc == NO_ERROR && IDBPolicy::rename(orig.c_str(), filename.c_str()) != 0)
              rc = ERR_COMP_RENAME_FILE;
          }

          // remove the tmp file
          string tmp(filename + ".tmp");

          if (rc == NO_ERROR && IDBPolicy::exists(tmp.c_str()) && IDBPolicy::remove(tmp.c_str()) != 0)
            rc = ERR_COMP_REMOVE_FILE;

          // remove the chunk shifting helper
          string rlc(filename + ".rlc");

          if (rc == NO_ERROR && IDBPolicy::exists(rlc.c_str()) && IDBPolicy::remove(rlc.c_str()) != 0)
            rc = ERR_COMP_REMOVE_FILE;

          logging::Message::Args args1;
          args1.add(filename);

          if (rc == NO_ERROR)
          {
            args1.add(" is restored.");
            SimpleSysLog::instance()->logMsg(args1, logging::LOG_TYPE_INFO, logging::M0007);
          }
          else
          {
            args1.add(" may not restored: ");
            args1.add(rc);
            SimpleSysLog::instance()->logMsg(args1, logging::LOG_TYPE_CRITICAL, logging::M0007);

            return rc;
          }
        }
        else
        {
          // copy back to the data file
          std::string backFileName(filename);

          if (backUpFileType.compare("chk") == 0)
            backFileName += ".chk";
          else
            backFileName += ".hdr";

          // cout << "Rollback found file " << backFileName << endl;
          IDBDataFile* sourceFile = IDBDataFile::open(
              IDBPolicy::getType(backFileName.c_str(), IDBPolicy::WRITEENG), backFileName.c_str(), "r", 0);
          IDBDataFile* targetFile = IDBDataFile::open(
              IDBPolicy::getType(filename.c_str(), IDBPolicy::WRITEENG), filename.c_str(), "r+", 0);

          size_t byteRead;
          unsigned char* readBuf = new unsigned char[size];
          boost::scoped_array<unsigned char> readBufPtr(readBuf);

          if (sourceFile != NULL)
          {
            int rc = sourceFile->seek(0, 0);

            if (rc)
              return ERR_FILE_SEEK;

            byteRead = sourceFile->read(readBuf, size);

            if ((int)byteRead != size)
            {
              logging::Message::Args args6;
              args6.add("Rollback cannot read backup file ");
              args6.add(backFileName);
              SimpleSysLog::instance()->logMsg(args6, logging::LOG_TYPE_ERROR, logging::M0007);
              return ERR_FILE_READ;
            }
          }
          else
          {
            logging::Message::Args args5;
            args5.add("Rollback cannot open backup file ");
            args5.add(backFileName);
            SimpleSysLog::instance()->logMsg(args5, logging::LOG_TYPE_ERROR, logging::M0007);
            return ERR_FILE_NULL;
          }

          size_t byteWrite;

          if (targetFile != NULL)
          {
            int rc = targetFile->seek(offset, 0);

            if (rc)
              return ERR_FILE_SEEK;

            byteWrite = targetFile->write(readBuf, size);

            if ((int)byteWrite != size)
            {
              logging::Message::Args args3;
              args3.add("Rollback cannot copy to file ");
              args3.add(filename);
              args3.add("from file ");
              args3.add(backFileName);
              SimpleSysLog::instance()->logMsg(args3, logging::LOG_TYPE_ERROR, logging::M0007);

              return ERR_FILE_WRITE;
            }
          }
          else
          {
            logging::Message::Args args4;
            args4.add("Rollback cannot open target file ");
            args4.add(filename);
            SimpleSysLog::instance()->logMsg(args4, logging::LOG_TYPE_ERROR, logging::M0007);
            return ERR_FILE_NULL;
          }

          // cout << "Rollback copied to file " << filename << " from file " << backFileName << endl;

          delete targetFile;
          delete sourceFile;
          IDBPolicy::remove(backFileName.c_str());
          logging::Message::Args arg1;
          arg1.add("Rollback copied to file ");
          arg1.add(filename);
          arg1.add("from file ");
          arg1.add(backFileName);
          SimpleSysLog::instance()->logMsg(arg1, logging::LOG_TYPE_INFO, logging::M0007);
        }
      }
    }

    IDBPolicy::remove(aDMLLogFileName.c_str());
  }

  return 0;
}

int WriteEngineWrapper::rollbackTran(const TxnID& txnid, int sessionId)
{
  if (rollbackCommon(txnid, sessionId) != 0)
    return -1;

  return BRMWrapper::getInstance()->rollBack(txnid, sessionId);
}

int WriteEngineWrapper::rollbackBlocks(const TxnID& txnid, int sessionId)
{
  if (rollbackCommon(txnid, sessionId) != 0)
    return -1;

  return BRMWrapper::getInstance()->rollBackBlocks(txnid, sessionId);
}

int WriteEngineWrapper::rollbackVersion(const TxnID& txnid, int sessionId)
{
  // BUG 4312
  RemoveTxnFromLBIDMap(txnid);
  RemoveTxnFromDictMap(txnid);

  return BRMWrapper::getInstance()->rollBackVersion(txnid, sessionId);
}

int WriteEngineWrapper::updateNextValue(const TxnID txnId, const OID& columnoid, const uint64_t nextVal,
                                        const uint32_t sessionID, const uint16_t dbRoot)
{
  int rc = NO_ERROR;
  boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr;
  RIDList ridList;
  ColValueList colValueList;
  WriteEngine::ColTupleList colTuples;
  ColStructList colStructList;
  WriteEngine::CSCTypesList cscColTypeList;
  WriteEngine::ColStruct colStruct;
  CalpontSystemCatalog::ColType colType;
  colType.columnOID = colStruct.dataOid = OID_SYSCOLUMN_NEXTVALUE;
  colType.colWidth = colStruct.colWidth = 8;
  colStruct.tokenFlag = false;
  colType.colDataType = colStruct.colDataType = CalpontSystemCatalog::UBIGINT;
  colStruct.fColDbRoot = dbRoot;

  m_opType = UPDATE;

  if (idbdatafile::IDBPolicy::useHdfs())
    colStruct.fCompressionType = 2;

  colStructList.push_back(colStruct);
  cscColTypeList.push_back(colType);
  ColTuple colTuple;
  systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  systemCatalogPtr->identity(CalpontSystemCatalog::EC);
  CalpontSystemCatalog::ROPair ropair;

  try
  {
    ropair = systemCatalogPtr->nextAutoIncrRid(columnoid);
  }
  catch (...)
  {
    rc = ERR_AUTOINC_RID;
  }

  if (rc != NO_ERROR)
    return rc;

  ridList.push_back(ropair.rid);
  colTuple.data = nextVal;
  colTuples.push_back(colTuple);
  colValueList.push_back(colTuples);
  rc = writeColumnRecords(txnId, cscColTypeList, colStructList, colValueList, ridList, SYSCOLUMN_BASE, false);

  if (rc != NO_ERROR)
    return rc;

  // flush PrimProc cache
  vector<LBID_t> blockList;
  BRM::LBIDRange_v lbidRanges;
  rc = BRMWrapper::getInstance()->lookupLbidRanges(OID_SYSCOLUMN_NEXTVALUE, lbidRanges);

  if (rc != NO_ERROR)
    return rc;

  LBIDRange_v::iterator it;

  for (it = lbidRanges.begin(); it != lbidRanges.end(); it++)
  {
    for (LBID_t lbid = it->start; lbid < (it->start + it->size); lbid++)
    {
      blockList.push_back(lbid);
    }
  }

  // Bug 5459 Flush FD cache
  std::vector<BRM::FileInfo> files;
  BRM::FileInfo aFile;
  aFile.oid = colStruct.dataOid;
  aFile.partitionNum = colStruct.fColPartition;
  aFile.dbRoot = colStruct.fColDbRoot;
  ;
  aFile.segmentNum = colStruct.fColSegment;
  aFile.compType = colStruct.fCompressionType;
  files.push_back(aFile);

  if (idbdatafile::IDBPolicy::useHdfs())
    cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID());

  rc = cacheutils::flushPrimProcAllverBlocks(blockList);

  if (rc != 0)
    rc = ERR_BLKCACHE_FLUSH_LIST;  // translate to WE error

  return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Flush compressed files in chunk manager
 * PARAMETERS:
 *    none
 * RETURN:
 *    none
 ***********************************************************/
int WriteEngineWrapper::flushDataFiles(int rc, const TxnID txnId, std::map<FID, FID>& columnOids)
{
  RemoveTxnFromLBIDMap(txnId);
  RemoveTxnFromDictMap(txnId);

  for (int i = 0; i < TOTAL_COMPRESS_OP; i++)
  {
    int rc1 = m_colOp[i]->flushFile(rc, columnOids);
    int rc2 = m_dctnry[i]->flushFile(rc, columnOids);

    if (rc == NO_ERROR)
    {
      rc = (rc1 != NO_ERROR) ? rc1 : rc2;
    }
  }

  return rc;
}

void WriteEngineWrapper::AddDictToList(const TxnID txnid, std::vector<BRM::LBID_t>& lbids)
{
  std::tr1::unordered_map<TxnID, dictLBIDRec_t>::iterator mapIter;

  mapIter = m_dictLBIDMap.find(txnid);

  if (mapIter == m_dictLBIDMap.end())
  {
    dictLBIDRec_t tempRecord;
    tempRecord.insert(lbids.begin(), lbids.end());
    m_dictLBIDMap[txnid] = tempRecord;
    return;
  }
  else
  {
    dictLBIDRec_t& txnRecord = mapIter->second;
    txnRecord.insert(lbids.begin(), lbids.end());
  }
}

// Get CPInfo for given starting LBID and column description structure.
int WriteEngineWrapper::GetLBIDRange(const BRM::LBID_t startingLBID, const ColStruct& colStruct,
                                     ExtCPInfo& cpInfo)
{
  int rtn;
  BRM::CPMaxMin maxMin;
  rtn = BRMWrapper::getInstance()->getExtentCPMaxMin(startingLBID, maxMin);
  bool isBinary = cpInfo.isBinaryColumn();
  maxMin.isBinaryColumn = isBinary;
  cpInfo.fCPInfo.firstLbid = startingLBID;
  if (rtn)
  {
    cpInfo.toInvalid();
    return rtn;
  }
  // if we are provided with CPInfo pointer to update, we record current range there.
  // please note that we may fail here for unknown extents - e.g., newly allocated ones.
  // for these we mark CPInfo as invalid (above) and proceed as usual.
  // XXX With this logic we may end with invalid ranges for extents that were
  //     allocated and recorded, yet not in our copy of extentmap.
  //     As we update only part of that extent, the recorded range will be for
  //     that part of the extent.
  //     It should be investigated whether such situation is possible.
  // XXX Please note that most if not all calls to AddLBIDToList are enclosed into
  //     RETURN_ON_ERROR() macro.
  //     If we have failed to obtain information above we will abort execution of the function
  //     that called us.
  //     This is potential source of bugs.
  cpInfo.fCPInfo.bigMax = maxMin.bigMax;
  cpInfo.fCPInfo.bigMin = maxMin.bigMin;
  cpInfo.fCPInfo.max = maxMin.max;
  cpInfo.fCPInfo.min = maxMin.min;
  cpInfo.fCPInfo.seqNum = maxMin.seqNum;
  cpInfo.fCPInfo.isBinaryColumn = maxMin.isBinaryColumn;
  return rtn;
}

/***********************************************************
 * DESCRIPTION:
 *    Add an lbid to a list of lbids for sending to markExtentsInvalid.
 *    However, rather than storing each lbid, store only unique first
 *    lbids. This is an optimization to prevent invalidating the same
 *    extents over and over.
 * PARAMETERS:
 *    txnid        - the lbid list is per txn. We use this to keep transactions
 *                   separated.
 *   These next are needed for dbrm to get the lbid:
 *    colStruct    - reference to ColStruct structure of column we record (provides segment and type).
 *    fbo          - file block offset
 *    cpInfo       - a CPInfo to collect if we need that.
 * RETURN: 0 => OK. -1 => error
 ***********************************************************/
int WriteEngineWrapper::AddLBIDtoList(const TxnID txnid, const ColStruct& colStruct, const int fbo,
                                      ExtCPInfo* cpInfo)
{
  int rtn = 0;

  BRM::LBID_t startingLBID;
  SP_TxnLBIDRec_t spTxnLBIDRec;
  std::tr1::unordered_map<TxnID, SP_TxnLBIDRec_t>::iterator mapIter;

  // Find the set of extent starting LBIDs for this transaction. If not found, then create it.
  mapIter = m_txnLBIDMap.find(txnid);

  if (mapIter == m_txnLBIDMap.end())
  {
    // This is a new transaction.
    SP_TxnLBIDRec_t sptemp(new TxnLBIDRec);
    spTxnLBIDRec = sptemp;
    m_txnLBIDMap[txnid] = spTxnLBIDRec;
    //        cout << "New transaction entry " << txnid << " transaction count " << m_txnLBIDMap.size() <<
    //        endl;
  }
  else
  {
    spTxnLBIDRec = (*mapIter).second;
  }

  // Get the extent starting lbid given all these values (startingLBID is an out parameter).
  rtn = BRMWrapper::getInstance()->getStartLbid(colStruct.dataOid, colStruct.fColPartition,
                                                colStruct.fColSegment, fbo, startingLBID);

  if (rtn != 0)
    return -1;

  // if we are given the cpInfo (column's ranges can be traced), we should update it.
  if (cpInfo)
  {
    rtn = GetLBIDRange(startingLBID, colStruct, *cpInfo);
  }
  else
  {
    spTxnLBIDRec->AddLBID(startingLBID, colStruct.colDataType);
  }

  return rtn;
}

void WriteEngineWrapper::RemoveTxnFromDictMap(const TxnID txnid)
{
  std::tr1::unordered_map<TxnID, dictLBIDRec_t>::iterator mapIter;

  mapIter = m_dictLBIDMap.find(txnid);

  if (mapIter != m_dictLBIDMap.end())
  {
    m_dictLBIDMap.erase(txnid);
  }
}

int WriteEngineWrapper::markTxnExtentsAsInvalid(const TxnID txnid, bool erase)
{
  int rc = 0;
  std::tr1::unordered_map<TxnID, SP_TxnLBIDRec_t>::iterator mapIter = m_txnLBIDMap.find(txnid);
  if (mapIter != m_txnLBIDMap.end())
  {
    SP_TxnLBIDRec_t spTxnLBIDRec = (*mapIter).second;
    if (!spTxnLBIDRec->m_LBIDs.empty())
    {
      rc = BRMWrapper::getInstance()->markExtentsInvalid(spTxnLBIDRec->m_LBIDs, spTxnLBIDRec->m_ColDataTypes);
    }

    if (erase)
    {
      m_txnLBIDMap.erase(txnid);  // spTxnLBIDRec is auto-destroyed
    }
  }
  return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Remove a transaction LBID list from the LBID map
 *    Called when a transaction ends, either commit or rollback
 * PARAMETERS:
 *    txnid - the transaction to remove.
 * RETURN:
 *    error code
 ***********************************************************/
int WriteEngineWrapper::RemoveTxnFromLBIDMap(const TxnID txnid)
{
  return markTxnExtentsAsInvalid(txnid, true);
}

}  // end of namespace
// vim:ts=4 sw=4:
