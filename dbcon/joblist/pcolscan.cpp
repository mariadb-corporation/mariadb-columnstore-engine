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

/***********************************************************************
 *   $Id: pcolscan.cpp 9655 2013-06-25 23:08:13Z xlou $
 *
 *
 ***********************************************************************/

#include <unistd.h>
#include <stdexcept>
#include <cstring>
#include <utility>
#include <sstream>
#include <cassert>
using namespace std;

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
using namespace boost;

#include "messagequeue.h"
using namespace messageqcpp;
#include "configcpp.h"
using namespace config;
#include "messageobj.h"
using namespace logging;

#include "logicoperator.h"
using namespace execplan;

#include "distributedenginecomm.h"
#include "primitivemsg.h"
#include "timestamp.h"
#include "unique32generator.h"
#include "jlf_common.h"
#include "primitivestep.h"

//#define DEBUG 1
//#define DEBUG2 1

namespace joblist
{
pColScanStep::pColScanStep(CalpontSystemCatalog::OID o, CalpontSystemCatalog::OID t,
                           const CalpontSystemCatalog::ColType& ct, const JobInfo& jobInfo)
 : JobStep(jobInfo)
 , fRm(jobInfo.rm)
 , fMsgHeader()
 , fFilterCount(0)
 , fOid(o)
 , fTableOid(t)
 , fColType(ct)
 , fBOP(BOP_OR)
 , fNumBlksSkipped(0)
 , fMsgBytesIn(0)
 , fMsgBytesOut(0)
 , fMsgsToPm(0)
{
  if (fTableOid == 0)  // cross engine support
    return;

  int err, i, mask;

  finishedSending = false;
  recvWaiting = 0;
  recvExited = 0;
  rDoNothing = false;
  fIsDict = false;

  // If this is a dictionary column, fudge the numbers...
  if (fColType.colDataType == CalpontSystemCatalog::VARCHAR)
  {
    if (8 > fColType.colWidth && 4 <= fColType.colWidth)
      fColType.colDataType = CalpontSystemCatalog::CHAR;

    fColType.colWidth++;
  }

  // If this is a dictionary column, fudge the numbers...
  if ((fColType.colDataType == CalpontSystemCatalog::VARBINARY) ||
      (fColType.colDataType == CalpontSystemCatalog::BLOB) ||
      (fColType.colDataType == CalpontSystemCatalog::TEXT))
  {
    fColType.colWidth = 8;
    fIsDict = true;
  }
  // MCOL-641 WIP
  else if (fColType.colWidth > 8 && fColType.colDataType != CalpontSystemCatalog::DECIMAL &&
           fColType.colDataType != CalpontSystemCatalog::UDECIMAL)
  {
    fColType.colWidth = 8;
    fIsDict = true;
    // TODO: is this right?
    fColType.colDataType = CalpontSystemCatalog::VARCHAR;
  }

  // Round colWidth up
  if (fColType.colWidth == 3)
    fColType.colWidth = 4;
  else if (fColType.colWidth == 5 || fColType.colWidth == 6 || fColType.colWidth == 7)
    fColType.colWidth = 8;

  err = dbrm.lookup(fOid, lbidRanges);

  if (err)
    throw runtime_error("pColScan: BRM LBID range lookup failure (1)");

  err = dbrm.getExtents(fOid, extents);

  if (err)
    throw runtime_error("pColScan: BRM HWM lookup failure (4)");

  sort(extents.begin(), extents.end(), BRM::ExtentSorter());
  numExtents = extents.size();
  extentSize = (fRm->getExtentRows() * fColType.colWidth) / BLOCK_SIZE;

  if (fOid > 3000)
  {
    lbidList.reset(new LBIDList(fOid, 0));
  }

  /* calculate shortcuts for rid-based arithmetic */
  for (i = 1, mask = 1; i <= 32; i++)
  {
    mask <<= 1;

    if (extentSize & mask)
    {
      divShift = i;
      break;
    }
  }

  for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
    if (extentSize & mask)
      throw runtime_error("pColScan: Extent size must be a power of 2 in blocks");

  ridsPerBlock = BLOCK_SIZE / fColType.colWidth;

  for (i = 1, mask = 1; i <= 32; i++)
  {
    mask <<= 1;

    if (ridsPerBlock & mask)
    {
      rpbShift = i;
      break;
    }
  }

  for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
    if (ridsPerBlock & mask)
      throw runtime_error("pColScan: Block size and column width must be a power of 2");
}

struct CPInfo
{
  CPInfo(int64_t MIN, int64_t MAX, uint64_t l) : min(MIN), max(MAX), LBID(l){};
  int64_t min;
  int64_t max;
  uint64_t LBID;
};

void pColScanStep::addFilter(int8_t COP, float value)
{
  fFilterString << (uint8_t)COP;
  fFilterString << (uint8_t)0;
  fFilterString << *((uint32_t*)&value);
  fFilterCount++;
}

void pColScanStep::addFilter(int8_t COP, int64_t value, uint8_t roundFlag)
{
  int8_t tmp8;
  int16_t tmp16;
  int32_t tmp32;

  fFilterString << (uint8_t)COP;
  fFilterString << roundFlag;

  // converts to a type of the appropriate width, then bitwise
  // copies into the filter ByteStream
  switch (fColType.colWidth)
  {
    case 1:
      tmp8 = value;
      fFilterString << *((uint8_t*)&tmp8);
      break;

    case 2:
      tmp16 = value;
      fFilterString << *((uint16_t*)&tmp16);
      break;

    case 4:
      tmp32 = value;
      fFilterString << *((uint32_t*)&tmp32);
      break;

    case 8: fFilterString << *((uint64_t*)&value); break;

    default:
      ostringstream o;

      o << "pColScanStep: CalpontSystemCatalog says OID " << fOid << " has a width of " << fColType.colWidth;
      throw runtime_error(o.str());
  }

  fFilterCount++;
}

const string pColScanStep::toString() const
{
  ostringstream oss;
  oss << "pColScanStep    ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId << " st:" << fStepId
      << " tb/col:" << fTableOid << "/" << fOid;

  if (alias().length())
    oss << " alias:" << alias();

  oss << " " << omitOidInDL << fOutputJobStepAssociation.outAt(0) << showOidInDL;
  oss << " nf:" << fFilterCount;
  oss << " in:";

  for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
  {
    oss << fInputJobStepAssociation.outAt(i) << ", ";
  }

  return oss.str();
}

uint64_t pColScanStep::getFBO(uint64_t lbid)
{
  uint32_t i;
  uint64_t lastLBID;

  for (i = 0; i < numExtents; i++)
  {
    lastLBID = extents[i].range.start + (extents[i].range.size << 10) - 1;

    if (lbid >= (uint64_t)extents[i].range.start && lbid <= lastLBID)
      return (lbid - extents[i].range.start) + (i << divShift);
  }

  cerr << "pColScan: didn't find the FBO?\n";
  throw logic_error("pColScan: didn't find the FBO?");
}

pColScanStep::pColScanStep(const pColStep& rhs) : JobStep(rhs), fRm(rhs.resourceManager()), fMsgHeader()
{
  fFilterCount = rhs.filterCount();
  fFilterString = rhs.filterString();
  isFilterFeeder = rhs.getFeederFlag();
  fOid = rhs.oid();
  fTableOid = rhs.tableOid();
  fColType = rhs.colType();
  fBOP = rhs.BOP();
  fIsDict = rhs.isDictCol();
  fNumBlksSkipped = 0;
  fMsgBytesIn = 0;
  fMsgBytesOut = 0;
  fMsgsToPm = 0;
  fCardinality = rhs.cardinality();
  fFilters = rhs.fFilters;
  fOnClauseFilter = rhs.onClauseFilter();

  if (fTableOid == 0)  // cross engine support
    return;

  int err;

  err = dbrm.lookup(fOid, lbidRanges);

  if (err)
    throw runtime_error("pColScan: BRM LBID range lookup failure (1)");

  err = dbrm.getExtents(fOid, extents);

  if (err)
    throw runtime_error("pColScan: BRM HWM lookup failure (4)");

  sort(extents.begin(), extents.end(), BRM::ExtentSorter());
  numExtents = extents.size();
  extentSize = (fRm->getExtentRows() * fColType.colWidth) / BLOCK_SIZE;
  lbidList = rhs.lbidList;
  finishedSending = sendWaiting = rDoNothing = false;
  recvWaiting = 0;
  recvExited = 0;

  /* calculate some shortcuts for extent-based arithmetic */
  ridsPerBlock = rhs.ridsPerBlock;
  rpbShift = rhs.rpbShift;
  divShift = rhs.divShift;

  fTraceFlags = rhs.fTraceFlags;
}

void pColScanStep::addFilters()
{
  AnyDataListSPtr dl = fInputJobStepAssociation.outAt(0);
  DataList_t* bdl = dl->dataList();
  idbassert(bdl);
  int it = -1;
  bool more;
  ElementType e;
  int64_t token;

  try
  {
    it = bdl->getIterator();
  }
  catch (std::exception& ex)
  {
    cerr << "pColScanStep::addFilters: caught exception: " << ex.what() << " stepno: " << fStepId << endl;
    throw;
  }
  catch (...)
  {
    cerr << "pColScanStep::addFilters: caught exception" << endl;
    throw;
  }

  fBOP = BOP_OR;
  more = bdl->next(it, &e);

  while (more)
  {
    token = e.second;
    addFilter(COMPARE_EQ, token);
    more = bdl->next(it, &e);
  }

  return;
}

bool pColScanStep::isEmptyVal(const uint8_t* val8) const
{
  const int width = fColType.colWidth;

  switch (fColType.colDataType)
  {
    case CalpontSystemCatalog::UTINYINT:
    {
      return (*val8 == joblist::UTINYINTEMPTYROW);
    }

    case CalpontSystemCatalog::USMALLINT:
    {
      const uint16_t* val16 = reinterpret_cast<const uint16_t*>(val8);
      return (*val16 == joblist::USMALLINTEMPTYROW);
    }

    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT:
    {
      const uint32_t* val32 = reinterpret_cast<const uint32_t*>(val8);
      return (*val32 == joblist::UINTEMPTYROW);
    }

    case CalpontSystemCatalog::UBIGINT:
    {
      const uint64_t* val64 = reinterpret_cast<const uint64_t*>(val8);
      return (*val64 == joblist::BIGINTEMPTYROW);
    }

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT:
    {
      const uint32_t* val32 = reinterpret_cast<const uint32_t*>(val8);
      return (*val32 == joblist::FLOATEMPTYROW);
    }

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE:
    {
      const uint64_t* val64 = reinterpret_cast<const uint64_t*>(val8);
      return (*val64 == joblist::DOUBLEEMPTYROW);
    }

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIME:
    case CalpontSystemCatalog::TIMESTAMP:
      if (width == 1)
      {
        return (*val8 == joblist::CHAR1EMPTYROW);
      }
      else if (width == 2)
      {
        const uint16_t* val16 = reinterpret_cast<const uint16_t*>(val8);
        return (*val16 == joblist::CHAR2EMPTYROW);
      }
      else if (width <= 4)
      {
        const uint32_t* val32 = reinterpret_cast<const uint32_t*>(val8);
        return (*val32 == joblist::CHAR4EMPTYROW);
      }
      else if (width <= 8)
      {
        const uint64_t* val64 = reinterpret_cast<const uint64_t*>(val8);
        return (*val64 == joblist::CHAR8EMPTYROW);
      }

    default: break;
  }

  switch (width)
  {
    case 1:
    {
      return (*val8 == joblist::TINYINTEMPTYROW);
    }

    case 2:
    {
      const uint16_t* val16 = reinterpret_cast<const uint16_t*>(val8);
      return (*val16 == joblist::SMALLINTEMPTYROW);
    }

    case 4:
    {
      const uint32_t* val32 = reinterpret_cast<const uint32_t*>(val8);
      return (*val32 == joblist::INTEMPTYROW);
    }

    case 8:
    {
      const uint64_t* val64 = reinterpret_cast<const uint64_t*>(val8);
      return (*val64 == joblist::BIGINTEMPTYROW);
    }

    default:
      MessageLog logger(LoggingID(28));
      logging::Message::Args colWidth;
      Message msg(33);

      colWidth.add(width);
      msg.format(colWidth);
      logger.logErrorMessage(msg);
      return false;
  }

  return false;
}

void pColScanStep::addFilter(const Filter* f)
{
  if (NULL != f)
    fFilters.push_back(f);
}

void pColScanStep::appendFilter(const std::vector<const execplan::Filter*>& fs)
{
  fFilters.insert(fFilters.end(), fs.begin(), fs.end());
}

}  // namespace joblist
// vim:ts=4 sw=4:
