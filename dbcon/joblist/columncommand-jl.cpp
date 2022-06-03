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

//
// $Id: columncommand-jl.cpp 9655 2013-06-25 23:08:13Z xlou $
// C++ Implementation: columncommand
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <string>
#include <cstdlib>
#include <sstream>
using namespace std;

#include "primitivestep.h"
#include "tablecolumn.h"
#include "bpp-jl.h"
#include "columncommand-jl.h"
#include "dbrm.h"

using namespace messageqcpp;

namespace joblist
{
ColumnCommandJL::ColumnCommandJL(const pColScanStep& scan, vector<BRM::LBID_t> lastLBID,
                                 bool hasAuxCol_, const std::vector<BRM::EMEntry>& extentsAux_,
                                 execplan::CalpontSystemCatalog::OID oidAux) :
                                extentsAux(extentsAux_), hasAuxCol(hasAuxCol_),
                                fOidAux(oidAux)
{
  BRM::DBRM dbrm;
  isScan = true;

  /* grab necessary vars from scan */
  traceFlags = scan.fTraceFlags;
  filterString = scan.fFilterString;
  filterCount = scan.fFilterCount;
  colType = scan.fColType;
  BOP = scan.fBOP;
  extents = scan.extents;
  OID = scan.fOid;
  colName = scan.fName;
  rpbShift = scan.rpbShift;
  fIsDict = scan.fIsDict;
  fLastLbid = lastLBID;

  // cout << "CCJL inherited lastlbids: ";
  // for (uint32_t i = 0; i < lastLBID.size(); i++)
  //	cout << lastLBID[i] << " ";
  // cout << endl;

  // 	cout << "Init columncommand from scan with OID " << OID << endl;
  /* I think modmask isn't necessary for scans */
  divShift = scan.divShift;
  modMask = (1 << divShift) - 1;

  // @Bug 2889.  Drop partition enhancement.  Read FilesPerColumnPartition and ExtentsPerSegmentFile for use
  // in RID calculation.
  fFilesPerColumnPartition = DEFAULT_FILES_PER_COLUMN_PARTITION;
  // MCOL-4685 remove the option to set more than 2 extents per file (ExtentsPreSegmentFile).
  fExtentsPerSegmentFile = DEFAULT_EXTENTS_PER_SEGMENT_FILE;
  config::Config* cf = config::Config::makeConfig();
  string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");

  if (fpc.length() != 0)
    fFilesPerColumnPartition = cf->uFromText(fpc);
}

ColumnCommandJL::ColumnCommandJL(const pColStep& step)
{
  BRM::DBRM dbrm;

  isScan = false;
  hasAuxCol = false;

  /* grab necessary vars from step */
  traceFlags = step.fTraceFlags;
  filterString = step.fFilterString;
  filterCount = step.fFilterCount;
  colType = step.fColType;
  BOP = step.fBOP;
  extents = step.extents;
  divShift = step.divShift;
  modMask = step.modMask;
  rpbShift = step.rpbShift;
  OID = step.fOid;
  colName = step.fName;
  fIsDict = step.fIsDict;
  ResourceManager* rm = ResourceManager::instance();
  numDBRoots = rm->getDBRootCount();

  // grab the last LBID for this column.  It's a _minor_ optimization for the block loader.
  // dbrm.getLastLocalHWM((BRM::OID_t)OID, dbroot, partNum, segNum, lastHWM);
  // dbrm.lookupLocal((BRM::OID_t)OID, partNum, segNum, lastHWM, fLastLbid);
  /*
  fLastLbid.resize(numDBRoots);
  for (uint32_t i = 0; i < numDBRoots; i++) {
      dbrm.getLastLocalHWM2((BRM::OID_t)OID, i+1, partNum, segNum, lastHWM);
      dbrm.lookupLocal((BRM::OID_t)OID, partNum, segNum, lastHWM, fLastLbid[i]);
  }
  */

  // 	cout << "Init columncommand from step with OID " << OID << " and width " << colType.colWidth << endl;

  // @Bug 2889.  Drop partition enhancement.  Read FilesPerColumnPartition and ExtentsPerSegmentFile for use
  // in RID calculation.
  fFilesPerColumnPartition = DEFAULT_FILES_PER_COLUMN_PARTITION;
  // MCOL-4685 remove the option to set more than 2 extents per file (ExtentsPreSegmentFile).
  fExtentsPerSegmentFile = DEFAULT_EXTENTS_PER_SEGMENT_FILE;
  config::Config* cf = config::Config::makeConfig();
  string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");

  if (fpc.length() != 0)
    fFilesPerColumnPartition = cf->uFromText(fpc);
}

ColumnCommandJL::ColumnCommandJL(const ColumnCommandJL& prevCmd, const DictStepJL& dictWithFilters)
{
  BRM::DBRM dbrm;

  /* grab necessary vars from scan */
  traceFlags = prevCmd.traceFlags;
  // we should call this constructor only when paired with dictionary
  // and in that case previous command should not have any filters and
  // should be "dict" (tokens) column command.
  idbassert(dictWithFilters.getFilterCount() == 0 || prevCmd.filterCount == 0);
  idbassert(prevCmd.fIsDict);

  // need to reencode filters.
  filterString = dictWithFilters.reencodedFilterString();
  // we have a limitation here.
  // consider this: textcol IS NULL AND textcol IN ('a', 'b')
  // XXX: should check.
  if (filterString.length() > 0 && (BOP = dictWithFilters.getBop() || prevCmd.filterString.length() < 1))
  {
    filterCount = dictWithFilters.getFilterCount();
    BOP = dictWithFilters.getBop();
    fContainsRanges = true;
  }
  else
  {
    filterCount = prevCmd.filterCount;
    filterString = prevCmd.filterString;
    BOP = prevCmd.BOP;
  }
  isScan = prevCmd.isScan;
  hasAuxCol = prevCmd.hasAuxCol;
  extentsAux = prevCmd.extentsAux;
  colType = prevCmd.colType;
  extents = prevCmd.extents;
  OID = prevCmd.OID;
  colName = prevCmd.colName;
  rpbShift = prevCmd.rpbShift;
  fIsDict = prevCmd.fIsDict;
  fLastLbid = prevCmd.fLastLbid;
  lbid = prevCmd.lbid;
  traceFlags = prevCmd.traceFlags;
  dbroot = prevCmd.dbroot;
  numDBRoots = prevCmd.numDBRoots;

  /* I think modmask isn't necessary for scans */
  divShift = prevCmd.divShift;
  modMask = (1 << divShift) - 1;

  // @Bug 2889.  Drop partition enhancement.  Read FilesPerColumnPartition and ExtentsPerSegmentFile for use
  // in RID calculation.
  fFilesPerColumnPartition = prevCmd.fFilesPerColumnPartition;
  // MCOL-4685 remove the option to set more than 2 extents per file (ExtentsPreSegmentFile).
  fExtentsPerSegmentFile = prevCmd.fExtentsPerSegmentFile;
}

ColumnCommandJL::~ColumnCommandJL()
{
}

// The other leg is in PP, its name is ColumnCommand::makeCommand.
void ColumnCommandJL::createCommand(ByteStream& bs) const
{
  bs << (uint8_t)COLUMN_COMMAND;
  colType.serialize(bs);
  bs << (uint8_t)isScan;
  bs << traceFlags;
  if (isDict() && fContainsRanges)
  {
    // XXX: we should discern here between IS (NOT) NULL and other filters.
    ByteStream empty;
    auto zeroFC = filterCount;
    bs << empty;
    bs << BOP;
    zeroFC = 0;
    bs << zeroFC;
  }
  else
  {
    bs << filterString;
    bs << BOP;
    bs << filterCount;
  }
  if (hasAuxCol)
    bs << (uint8_t)1;
  else
    bs << (uint8_t)0;
  serializeInlineVector(bs, fLastLbid);

  CommandJL::createCommand(bs);
}

void ColumnCommandJL::runCommand(ByteStream& bs) const
{
  bs << lbid;

  if (hasAuxCol)
    bs << lbidAux;
}

void ColumnCommandJL::setLBID(uint64_t rid, uint32_t dbRoot)
{
  uint32_t partNum;
  uint16_t segNum;
  uint8_t extentNum;
  uint16_t blockNum;
  uint32_t colWidth;
  uint32_t i;

  idbassert(extents.size() > 0);
  colWidth = extents[0].colWid;
  rowgroup::getLocationFromRid(rid, &partNum, &segNum, &extentNum, &blockNum);

  for (i = 0; i < extents.size(); i++)
  {
    if (extents[i].dbRoot == dbRoot && extents[i].partitionNum == partNum &&
        extents[i].segmentNum == segNum && extents[i].blockOffset == (extentNum * colWidth * 1024))
    {
      lbid = extents[i].range.start + (blockNum * colWidth);
      currentExtentIndex = i;
      /*
      ostringstream os;
      os << "CCJL: rid=" << rid << "; dbroot=" << dbRoot << "; partitionNum=" << partNum
          << "; segmentNum=" << segNum <<	"; extentNum = " << (int) extentNum <<
          "; blockNum = " << blockNum << "; OID=" << OID << " LBID=" << lbid;
      cout << os.str() << endl;
      */
      break;
    }
  }

  uint32_t j;

  for (j = 0; j < extentsAux.size(); j++)
  {
    if (extentsAux[j].dbRoot == dbRoot && extentsAux[j].partitionNum == partNum &&
        extentsAux[j].segmentNum == segNum && extentsAux[j].blockOffset == (extentNum * 1 * 1024))
    {
      lbidAux = extentsAux[j].range.start + (blockNum * 1);
      break;
    }
  }

  if (i == extents.size() || (hasAuxCol && j == extentsAux.size()))
  {
    throw logic_error("ColumnCommandJL: setLBID didn't find the extent for the rid.");
  }

  //		ostringstream os;
  //		os << "CCJL: rid=" << rid << "; dbroot=" << dbRoot << "; partitionNum=" << partitionNum << ";
  //segmentNum=" << segmentNum << "; stripeWithinPartition=" << 			stripeWithinPartition << "; OID=" << OID << "
  //LBID=" << lbid; 		BRM::log(os.str());
}

inline uint32_t ColumnCommandJL::getFBO(uint64_t lbid)
{
  uint32_t i;
  uint64_t lastLBID;

  for (i = 0; i < extents.size(); i++)
  {
    lastLBID = extents[i].range.start + (extents[i].range.size << 10) - 1;

    if (lbid >= (uint64_t)extents[i].range.start && lbid <= lastLBID)
    {
      // @Bug 2889.  Change for drop partition.  Treat the FBO as if all of the partitions are
      // still there as one or more partitions may have been dropped. Get the original index
      // for this partition (i.e. what the index would be if all of the partitions were still
      // there).  The RIDs wind up being calculated off of this FBO for use in DML and DML
      // needs calculates the partition number, segment number, etc. off of the RID and needs
      // to remain the same when partitions are dropped.
      //
      // originalIndex = extents in partitions above +
      //		   extents in this partition in stripes above +
      //		   file offset in this stripe
      uint32_t originalIndex =
          (extents[i].partitionNum * fExtentsPerSegmentFile * fFilesPerColumnPartition) +
          ((extents[i].blockOffset / extents[i].colWid / 1024) * fFilesPerColumnPartition) +
          extents[i].segmentNum;

      return (lbid - extents[i].range.start) + (originalIndex << divShift);
    }
  }

  throw logic_error("ColumnCommandJL: didn't find the FBO?");
}

uint8_t ColumnCommandJL::getTableColumnType()
{
  switch (colType.colWidth)
  {
    case 8: return TableColumn::UINT64;

    case 4: return TableColumn::UINT32;

    case 2: return TableColumn::UINT16;

    case 1: return TableColumn::UINT8;

    // TODO MCOL-641
    case 16:
      // fallthrough

    default: throw logic_error("ColumnCommandJL: bad column width");
  }
}

string ColumnCommandJL::toString()
{
  ostringstream ret;

  ret << "ColumnCommandJL: " << filterCount << " filters, BOP=" << ((int)BOP) << ", colwidth=" << colType.colWidth << " oid=" << OID
      << " name=" << colName;

  if (isScan)
    ret << " (scan)";

  if (isDict())
    ret << " (tokens)";
  else if (datatypes::isCharType(colType.colDataType))
    ret << " (is char)";

  return ret.str();
}

uint16_t ColumnCommandJL::getWidth()
{
  return colType.colWidth;
}

void ColumnCommandJL::reloadExtents()
{
  int err;
  BRM::DBRM dbrm;

  err = dbrm.getExtents(OID, extents);

  if (err)
  {
    ostringstream os;
    os << "pColStep: BRM lookup error. Could not get extents for OID " << OID;
    throw runtime_error(os.str());
  }

  sort(extents.begin(), extents.end(), BRM::ExtentSorter());

  if (hasAuxCol)
  {
    err = dbrm.getExtents(fOidAux, extentsAux);

    if (err)
    {
      ostringstream os;
      os << "BRM lookup error. Could not get extents for Aux OID " << fOidAux;
      throw runtime_error(os.str());
    }

    sort(extentsAux.begin(), extentsAux.end(), BRM::ExtentSorter());
  }
}

bool ColumnCommandJL::getIsDict()
{
  return fIsDict;
}

};  // namespace joblist
