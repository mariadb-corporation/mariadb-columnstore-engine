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
 *   $Id: pcolstep.cpp 9655 2013-06-25 23:08:13Z xlou $
 *
 *
 ***********************************************************************/
#include <sstream>
#include <iomanip>
#include <algorithm>
//#define NDEBUG
#include <cassert>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
using namespace std;

#include "distributedenginecomm.h"
#include "elementtype.h"
#include "unique32generator.h"

#include "messagequeue.h"
using namespace messageqcpp;
#include "configcpp.h"
using namespace config;

#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
using namespace logging;

#include "calpontsystemcatalog.h"
#include "logicoperator.h"
using namespace execplan;

#include "brm.h"
using namespace BRM;

#include "idbcompress.h"
#include "jlf_common.h"
#include "primitivestep.h"

// #define DEBUG 1

namespace joblist
{
pColStep::pColStep(CalpontSystemCatalog::OID o, CalpontSystemCatalog::OID t,
                   const CalpontSystemCatalog::ColType& ct, const JobInfo& jobInfo)
 : JobStep(jobInfo)
 , fRm(jobInfo.rm)
 , sysCat(jobInfo.csc)
 , fOid(o)
 , fTableOid(t)
 , fColType(ct)
 , fFilterCount(0)
 , fBOP(BOP_NONE)
 , ridList(0)
 , msgsSent(0)
 , msgsRecvd(0)
 , finishedSending(false)
 , recvWaiting(false)
 , fIsDict(false)
 , isEM(jobInfo.isExeMgr)
 , ridCount(0)
 , fSwallowRows(false)
 , isFilterFeeder(false)
 , fNumBlksSkipped(0)
 , fMsgBytesIn(0)
 , fMsgBytesOut(0)
{
  if (fTableOid == 0)  // cross engine support
    return;

  int err, i;
  uint32_t mask;

  if (fOid < 1000)
    throw runtime_error("pColStep: invalid column");

  if (!compress::CompressInterface::isCompressionAvail(fColType.compressionType))
  {
    ostringstream oss;
    oss << "Unsupported compression type " << fColType.compressionType;
    oss << " for " << sysCat->colName(fOid);
#ifdef SKIP_IDB_COMPRESSION
    oss << ". It looks you're running Community binaries on an Enterprise database.";
#endif
    throw runtime_error(oss.str());
  }

  realWidth = fColType.colWidth;

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
  // WIP MCOL-641
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

  idbassert(fColType.colWidth > 0);
  ridsPerBlock = BLOCK_SIZE / fColType.colWidth;

  /* calculate some shortcuts for extent and block based arithmetic */
  extentSize = (fRm->getExtentRows() * fColType.colWidth) / BLOCK_SIZE;

  for (i = 1, mask = 1, modMask = 0; i <= 32; i++)
  {
    mask <<= 1;
    modMask = (modMask << 1) | 1;

    if (extentSize & mask)
    {
      divShift = i;
      break;
    }
  }

  for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
    if (extentSize & mask)
      throw runtime_error("pColStep: Extent size must be a power of 2 in blocks");

  /* calculate shortcuts for rid-based arithmetic */
  for (i = 1, mask = 1, rpbMask = 0; i <= 32; i++)
  {
    mask <<= 1;
    rpbMask = (rpbMask << 1) | 1;

    if (ridsPerBlock & mask)
    {
      rpbShift = i;
      break;
    }
  }

  for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
    if (ridsPerBlock & mask)
      throw runtime_error("pColStep: Block size and column width must be a power of 2");

  for (i = 0, mask = 1, blockSizeShift = 0; i < 32; i++)
  {
    if (mask == BLOCK_SIZE)
    {
      blockSizeShift = i;
      break;
    }

    mask <<= 1;
  }

  if (i == 32)
    throw runtime_error("pColStep: Block size must be a power of 2");

  err = dbrm.getExtents(o, extents);

  if (err)
  {
    ostringstream os;
    os << "pColStep: BRM lookup error. Could not get extents for OID " << o;
    throw runtime_error(os.str());
  }

  if (fOid > 3000)
  {
    lbidList.reset(new LBIDList(fOid, 0));
  }

  sort(extents.begin(), extents.end(), ExtentSorter());
  numExtents = extents.size();
  //	uniqueID = UniqueNumberGenerator::instance()->getUnique32();
  //	if (fDec)
  //		fDec->addQueue(uniqueID);
  // 	initializeConfigParms ( );
}

pColStep::pColStep(const pColScanStep& rhs)
 : JobStep(rhs)
 , fRm(rhs.resourceManager())
 , fOid(rhs.oid())
 , fTableOid(rhs.tableOid())
 , fColType(rhs.colType())
 , fFilterCount(rhs.filterCount())
 , fBOP(rhs.BOP())
 , ridList(0)
 , fFilterString(rhs.filterString())
 , msgsSent(0)
 , msgsRecvd(0)
 , finishedSending(false)
 , recvWaiting(false)
 , fIsDict(rhs.isDictCol())
 , ridCount(0)
 , fSwallowRows(false)
 , fNumBlksSkipped(0)
 , fMsgBytesIn(0)
 , fMsgBytesOut(0)
 , fFilters(rhs.getFilters())
{
  int err, i;
  uint32_t mask;

  if (fTableOid == 0)  // cross engine support
    return;

  if (fOid < 1000)
    throw runtime_error("pColStep: invalid column");

  ridsPerBlock = rhs.getRidsPerBlock();
  /* calculate some shortcuts for extent and block based arithmetic */
  extentSize = (fRm->getExtentRows() * fColType.colWidth) / BLOCK_SIZE;

  for (i = 1, mask = 1, modMask = 0; i <= 32; i++)
  {
    mask <<= 1;
    modMask = (modMask << 1) | 1;

    if (extentSize & mask)
    {
      divShift = i;
      break;
    }
  }

  for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
    if (extentSize & mask)
      throw runtime_error("pColStep: Extent size must be a power of 2 in blocks");

  /* calculate shortcuts for rid-based arithmetic */
  for (i = 1, mask = 1, rpbMask = 0; i <= 32; i++)
  {
    mask <<= 1;
    rpbMask = (rpbMask << 1) | 1;

    if (ridsPerBlock & mask)
    {
      rpbShift = i;
      break;
    }
  }

  for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
    if (ridsPerBlock & mask)
      throw runtime_error("pColStep: Block size and column width must be a power of 2");

  for (i = 0, mask = 1, blockSizeShift = 0; i < 32; i++)
  {
    if (mask == BLOCK_SIZE)
    {
      blockSizeShift = i;
      break;
    }

    mask <<= 1;
  }

  if (i == 32)
    throw runtime_error("pColStep: Block size must be a power of 2");

  err = dbrm.getExtents(fOid, extents);

  if (err)
  {
    ostringstream os;
    os << "pColStep: BRM lookup error. Could not get extents for OID " << fOid;
    throw runtime_error(os.str());
  }

  lbidList = rhs.getlbidList();

  sort(extents.begin(), extents.end(), ExtentSorter());
  numExtents = extents.size();

  fOnClauseFilter = rhs.onClauseFilter();

  //	uniqueID = UniqueNumberGenerator::instance()->getUnique32();
  //	if (fDec)
  //		fDec->addQueue(uniqueID);
  // 	initializeConfigParms ( );
}

pColStep::pColStep(const PassThruStep& rhs)
 : JobStep(rhs)
 , fRm(rhs.resourceManager())
 , fOid(rhs.oid())
 , fTableOid(rhs.tableOid())
 , fColType(rhs.colType())
 , fFilterCount(0)
 , fBOP(BOP_NONE)
 , ridList(0)
 , msgsSent(0)
 , msgsRecvd(0)
 , finishedSending(false)
 , recvWaiting(false)
 , fIsDict(rhs.isDictCol())
 , ridCount(0)
 , fSwallowRows(false)
 , fNumBlksSkipped(0)
 , fMsgBytesIn(0)
 , fMsgBytesOut(0)
{
  int err, i;
  uint32_t mask;

  if (fTableOid == 0)  // cross engine support
    return;

  if (fOid < 1000)
    throw runtime_error("pColStep: invalid column");

  ridsPerBlock = BLOCK_SIZE / fColType.colWidth;
  /* calculate some shortcuts for extent and block based arithmetic */
  extentSize = (fRm->getExtentRows() * fColType.colWidth) / BLOCK_SIZE;

  for (i = 1, mask = 1, modMask = 0; i <= 32; i++)
  {
    mask <<= 1;
    modMask = (modMask << 1) | 1;

    if (extentSize & mask)
    {
      divShift = i;
      break;
    }
  }

  for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
    if (extentSize & mask)
      throw runtime_error("pColStep: Extent size must be a power of 2 in blocks");

  /* calculate shortcuts for rid-based arithmetic */
  for (i = 1, mask = 1, rpbMask = 0; i <= 32; i++)
  {
    mask <<= 1;
    rpbMask = (rpbMask << 1) | 1;

    if (ridsPerBlock & mask)
    {
      rpbShift = i;
      break;
    }
  }

  for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
    if (ridsPerBlock & mask)
      throw runtime_error("pColStep: Block size and column width must be a power of 2");

  for (i = 0, mask = 1, blockSizeShift = 0; i < 32; i++)
  {
    if (mask == BLOCK_SIZE)
    {
      blockSizeShift = i;
      break;
    }

    mask <<= 1;
  }

  if (i == 32)
    throw runtime_error("pColStep: Block size must be a power of 2");

  err = dbrm.getExtents(fOid, extents);

  if (err)
  {
    ostringstream os;
    os << "pColStep: BRM lookup error. Could not get extents for OID " << fOid;
    throw runtime_error(os.str());
  }

  sort(extents.begin(), extents.end(), ExtentSorter());
  numExtents = extents.size();
}

void pColStep::addFilter(int8_t COP, float value)
{
  fFilterString << (uint8_t)COP;
  fFilterString << (uint8_t)0;
  fFilterString << *((uint32_t*)&value);
  fFilterCount++;
}

void pColStep::addFilter(int8_t COP, int64_t value, uint8_t roundFlag)
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

      o << "pColStep: CalpontSystemCatalog says OID " << fOid << " has a width of " << fColType.colWidth;
      throw runtime_error(o.str());
  }

  fFilterCount++;
}

void pColStep::addFilter(int8_t COP, const int128_t& value, uint8_t roundFlag)
{
  fFilterString << (uint8_t)COP;
  fFilterString << roundFlag;

  // bitwise copies into the filter ByteStream
  fFilterString << *reinterpret_cast<const uint128_t*>(&value);

  fFilterCount++;
}

const string pColStep::toString() const
{
  ostringstream oss;
  oss << "pColStep        ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId << " st:" << fStepId
      << " tb/col:" << fTableOid << "/" << fOid;

  if (alias().length())
    oss << " alias:" << alias();

  if (view().length())
    oss << " view:" << view();

  if (fOutputJobStepAssociation.outSize() > 0)
    oss << " " << omitOidInDL << fOutputJobStepAssociation.outAt(0) << showOidInDL;
  else
    oss << " (no output yet)";

  oss << " nf:" << fFilterCount;
  oss << " in:";

  for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
  {
    oss << fInputJobStepAssociation.outAt(i) << ", ";
  }

  if (fSwallowRows)
    oss << " (sink)";

  return oss.str();
}

void pColStep::addFilters()
{
  AnyDataListSPtr dl = fInputJobStepAssociation.outAt(0);
  DataList_t* bdl = dl->dataList();
  FifoDataList* fifo = fInputJobStepAssociation.outAt(0)->fifoDL();

  idbassert(bdl);
  int it = -1;
  bool more;
  ElementType e;
  int64_t token;

  if (fifo != NULL)
  {
    try
    {
      it = fifo->getIterator();
    }
    catch (exception& ex)
    {
      cerr << "pColStep::addFilters: caught exception: " << ex.what() << " stepno: " << fStepId << endl;
    }
    catch (...)
    {
      cerr << "pColStep::addFilters: caught exception" << endl;
    }

    fBOP = BOP_OR;
    UintRowGroup rw;

    more = fifo->next(it, &rw);

    while (more)
    {
      for (uint64_t i = 0; i < rw.count; ++i)
        addFilter(COMPARE_EQ, (int64_t)rw.et[i].second);

      more = fifo->next(it, &rw);
    }
  }
  else
  {
    try
    {
      it = bdl->getIterator();
    }
    catch (exception& ex)
    {
      cerr << "pColStep::addFilters: caught exception: " << ex.what() << " stepno: " << fStepId << endl;
    }
    catch (...)
    {
      cerr << "pColStep::addFilters: caught exception" << endl;
    }

    fBOP = BOP_OR;

    more = bdl->next(it, &e);

    while (more)
    {
      token = e.second;
      addFilter(COMPARE_EQ, token);

      more = bdl->next(it, &e);
    }
  }

  return;
}

/* This exists to avoid a DBRM lookup for every rid. */
inline uint64_t pColStep::getLBID(uint64_t rid, bool& scan)
{
  uint32_t extentIndex, extentOffset;
  uint64_t fbo;
  fbo = rid >> rpbShift;
  extentIndex = fbo >> divShift;
  extentOffset = fbo & modMask;
  scan = (scanFlags[extentIndex] != 0);
  return extents[extentIndex].range.start + extentOffset;
}

inline uint64_t pColStep::getFBO(uint64_t lbid)
{
  uint32_t i;
  uint64_t lastLBID;

  for (i = 0; i < numExtents; i++)
  {
    lastLBID = extents[i].range.start + (extents[i].range.size << 10) - 1;

    if (lbid >= (uint64_t)extents[i].range.start && lbid <= lastLBID)
      return (lbid - extents[i].range.start) + (i << divShift);
  }

  cerr << "pColStep: didn't find the FBO?\n";
  throw logic_error("pColStep: didn't find the FBO?");
}

void pColStep::appendFilter(const messageqcpp::ByteStream& filter, unsigned count)
{
  fFilterString += filter;
  fFilterCount += count;
}

void pColStep::addFilter(const Filter* f)
{
  if (NULL != f)
    fFilters.push_back(f);
}

void pColStep::appendFilter(const std::vector<const execplan::Filter*>& fs)
{
  fFilters.insert(fFilters.end(), fs.begin(), fs.end());
}

}  // namespace joblist
// vim:ts=4 sw=4:
