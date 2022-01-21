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

//
// $Id: columncommand.cpp 2057 2013-02-13 17:00:10Z pleblanc $
// C++ Implementation: columncommand
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
// Copyright: See COPYING file that comes with this distribution
//
//

#include <unistd.h>
#include <sstream>
#include <map>
#include <cstdlib>
#include <cmath>
using namespace std;

#include "bpp.h"
#include "errorcodes.h"
#include "exceptclasses.h"
#include "primitiveserver.h"
#include "primproc.h"
#include "stats.h"
#include "datatypes/mcs_int128.h"

using namespace messageqcpp;
using namespace rowgroup;

#include "messageids.h"
using namespace logging;

#ifdef _MSC_VER
#define llabs labs
#endif

namespace primitiveprocessor
{
extern int noVB;

ColumnCommand::ColumnCommand() : Command(COLUMN_COMMAND), blockCount(0), loadCount(0), suppressFilter(false)
{
}

ColumnCommand::ColumnCommand(execplan::CalpontSystemCatalog::ColType& aColType) : ColumnCommand()
{
  colType = aColType;
}

ColumnCommand::~ColumnCommand()
{
}

void ColumnCommand::_execute()
{
  if (_isScan)
    makeScanMsg();
  else if (bpp->ridCount == 0)  // this would cause a scan
  {
    blockCount += colType.colWidth;
    return;  // a step with no input rids does nothing
  }
  else
    makeStepMsg();

  issuePrimitive();
  processResult();

  // check if feeding a filtercommand
  if (fFilterFeeder != NOT_FEEDER)
    copyRidsForFilterCmd();
}

void ColumnCommand::execute()
{
  if (fFilterFeeder == LEFT_FEEDER)
  {
    values = bpp->fFiltCmdValues[0].get();
    wide128Values = bpp->fFiltCmdBinaryValues[0].get();
  }
  else if (fFilterFeeder == RIGHT_FEEDER)
  {
    values = bpp->fFiltCmdValues[1].get();
    wide128Values = bpp->fFiltCmdBinaryValues[1].get();
  }
  else
  {
    values = bpp->values;
    wide128Values = bpp->wide128Values;
  }

  _execute();
}

void ColumnCommand::execute(int64_t* vals)
{
  values = vals;
  _execute();
}

void ColumnCommand::makeScanMsg()
{
  /* Finish the NewColRequestHeader. */

  /* XXXPAT: if there is a Command preceeding this one, it's a DictScan feeding tokens
  which need to become filter elements.  Can we handle that with this design?
  Implement that later. */

  primMsg->ism.Size = baseMsgLength;
  primMsg->NVALS = 0;
  primMsg->LBID = lbid;
  primMsg->RidFlags = 0xFFFF;

  // 	cout << "scanning lbid " << lbid << " colwidth = " << primMsg->DataSize <<
  // 		" filterCount = " << filterCount << " outputType = " <<
  // 		(int) primMsg->OutputType << endl;
}

void ColumnCommand::makeStepMsg()
{
  memcpy(&inputMsg[baseMsgLength], bpp->relRids, bpp->ridCount << 1);
  primMsg->RidFlags = bpp->ridMap;
  primMsg->ism.Size = baseMsgLength + (bpp->ridCount << 1);
  primMsg->NVALS = bpp->ridCount;
  primMsg->LBID = lbid;
  // 	cout << "lbid is " << lbid << endl;
}

template <int W>
void ColumnCommand::_loadData()
{
  using ByteStreamType = typename messageqcpp::ByteStreamType<W>::type;
  uint32_t wasCached;
  uint32_t blocksRead;
  uint16_t _mask;
  uint64_t oidLastLbid = 0;
  bool lastBlockReached = false;
  oidLastLbid = getLastLbid();
  uint32_t blocksToLoad = 0;
  // The number of elements allocated equals to the number of
  // iteratations of the first loop here.
  BRM::LBID_t* lbids = (BRM::LBID_t*)alloca(W * sizeof(BRM::LBID_t));
  uint8_t** blockPtrs = (uint8_t**)alloca(W * sizeof(uint8_t*));
  int i;

  _mask = mask;
  // primMsg->RidFlags = 0xffff;   // disables selective block loading
  // cerr << "::ColumnCommand::_loadData OID " << getOID() << " l:" << primMsg->LBID << " ll: " << oidLastLbid
  // << " primMsg->RidFlags " << primMsg->RidFlags << endl;

  for (i = 0; i < W; ++i, _mask <<= shift)
  {
    if ((!lastBlockReached && _isScan) || (!_isScan && primMsg->RidFlags & _mask))
    {
      lbids[blocksToLoad] = primMsg->LBID + i;
      blockPtrs[blocksToLoad] = &bpp->blockData[i * BLOCK_SIZE];
      blocksToLoad++;
      loadCount++;
    }
    else if (lastBlockReached && _isScan)
    {
      // fill remaining blocks with empty values when col scan
      uint32_t blockLen = BLOCK_SIZE / W;
      auto attrs = datatypes::SystemCatalog::TypeAttributesStd(W, 0, -1);
      const auto* typeHandler = datatypes::TypeHandler::find(colType.colDataType, attrs);
      const uint8_t* emptyValue = typeHandler->getEmptyValueForType(attrs);
      uint8_t* blockDataPtr = &bpp->blockData[i * BLOCK_SIZE];

      idbassert(blockDataPtr);
      fillEmptyBlock<ByteStreamType>(blockDataPtr, emptyValue, blockLen);
    }  // else

    if ((primMsg->LBID + i) == oidLastLbid)
      lastBlockReached = true;

    blockCount++;
  }  // for

  /* Do the load */
  wasCached = primitiveprocessor::loadBlocks(lbids, bpp->versionInfo, bpp->txnID, colType.compressionType,
                                             blockPtrs, &blocksRead, bpp->LBIDTrace, bpp->sessionID,
                                             blocksToLoad, &wasVersioned, willPrefetch(), &bpp->vssCache);
  bpp->cachedIO += wasCached;
  bpp->physIO += blocksRead;
  bpp->touchedBlocks += blocksToLoad;
}

void ColumnCommand::loadData()
{
  switch (colType.colWidth)
  {
    case 1: _loadData<1>(); break;
    case 2: _loadData<2>(); break;
    case 4: _loadData<4>(); break;
    case 8: _loadData<8>(); break;
    case 16: _loadData<16>(); break;

    default:
      throw NotImplementedExcept(std::string("ColumnCommand::loadData does not support ") +
                                 std::to_string(colType.colWidth) + std::string(" byte width."));
  }
}

void ColumnCommand::issuePrimitive()
{
  loadData();

  if (!suppressFilter)
    bpp->getPrimitiveProcessor().setParsedColumnFilter(parsedColumnFilter);
  else
    bpp->getPrimitiveProcessor().setParsedColumnFilter(emptyFilter);

  switch (colType.colWidth)
  {
    case 1: _issuePrimitive<1>(); break;
    case 2: _issuePrimitive<2>(); break;
    case 4: _issuePrimitive<4>(); break;
    case 8: _issuePrimitive<8>(); break;
    case 16: _issuePrimitive<16>(); break;
    default:
      throw NotImplementedExcept(std::string("ColumnCommand::_issuePrimitive does not support ") +
                                 std::to_string(colType.colWidth) + std::string(" byte width."));
  }

  if (_isScan)
  {
    if (LIKELY(colType.isNarrow()))
      updateCPDataNarrow();
    else
      updateCPDataWide();
  }
}

template <int W>
void ColumnCommand::_issuePrimitiveNarrow()
{
  _loadData<W>();

  if (!suppressFilter)
    bpp->getPrimitiveProcessor().setParsedColumnFilter(parsedColumnFilter);
  else
    bpp->getPrimitiveProcessor().setParsedColumnFilter(emptyFilter);

  _issuePrimitive<W>();

  if (_isScan)
    updateCPDataNarrow();
}

template <int W>
void ColumnCommand::_issuePrimitive()
{
  using IntegralType = typename datatypes::WidthToSIntegralType<W>::type;
  // Down the call stack the code presumes outMsg buffer has enough space to store
  // ColRequestHeader + uint16_t Rids[8192] + IntegralType[8192].
  bpp->getPrimitiveProcessor().columnScanAndFilter<IntegralType>(primMsg, outMsg);
}  // _issuePrimitive()

void ColumnCommand::updateCPDataNarrow()
{
  /* Update CP data, the PseudoColumn code should always be !_isScan.  Should be safe
      to leave this here for now. */
  if (_isScan)
  {
    bpp->validCPData = (outMsg->ValidMinMax && !wasVersioned);
    bpp->lbidForCP = lbid;
    bpp->maxVal = static_cast<int64_t>(outMsg->Max);
    bpp->minVal = static_cast<int64_t>(outMsg->Min);
  }
}

void ColumnCommand::updateCPDataWide()
{
  /* Update CP data, the PseudoColumn code should always be !_isScan.  Should be safe
      to leave this here for now. */
  if (_isScan)
  {
    bpp->validCPData = (outMsg->ValidMinMax && !wasVersioned);
    bpp->lbidForCP = lbid;
    if (colType.isWideDecimalType())
    {
      bpp->hasWideColumnOut = true;
      // colWidth is int32 and wideColumnWidthOut's
      // value is expected to be at most uint8.
      bpp->wideColumnWidthOut = colType.colWidth;
      bpp->max128Val = outMsg->Max;
      bpp->min128Val = outMsg->Min;
    }
    else
    {
      throw NotImplementedExcept("ColumnCommand::updateCPDataWide(): unsupported dataType " +
                                 std::to_string(primMsg->colType.DataType) + " with width " +
                                 std::to_string(colType.colWidth));
    }
  }
}

template <int W>
void ColumnCommand::_process_OT_BOTH_wAbsRids()
{
  using T = typename datatypes::WidthToSIntegralType<W>::type;
  bpp->ridCount = outMsg->NVALS;
  bpp->ridMap = outMsg->RidFlags;
  uint8_t* outPtr = reinterpret_cast<uint8_t*>(&outMsg[1]);
  auto* ridPos = primitives::getRIDArrayPosition(outPtr, 0);
  T* valuesPos = primitives::getValuesArrayPosition<T>(primitives::getFirstValueArrayPosition(outMsg), 0);

  for (size_t i = 0; i < outMsg->NVALS; ++i)
  {
    bpp->relRids[i] = ridPos[i];
    bpp->absRids[i] = ridPos[i] + bpp->baseRid;
    values[i] = valuesPos[i];
  }
}

template <>
void ColumnCommand::_process_OT_BOTH_wAbsRids<16>()
{
  using T = typename datatypes::WidthToSIntegralType<16>::type;
  bpp->ridCount = outMsg->NVALS;
  bpp->ridMap = outMsg->RidFlags;
  uint8_t* outPtr = reinterpret_cast<uint8_t*>(&outMsg[1]);
  auto* ridPos = primitives::getRIDArrayPosition(outPtr, 0);
  int128_t* valuesPos =
      primitives::getValuesArrayPosition<T>(primitives::getFirstValueArrayPosition(outMsg), 0);

  for (size_t i = 0; i < outMsg->NVALS; ++i)
  {
    bpp->relRids[i] = ridPos[i];
    bpp->absRids[i] = ridPos[i] + bpp->baseRid;
    datatypes::TSInt128::assignPtrPtr(&wide128Values[i], &valuesPos[i]);
  }
}

template <int W>
void ColumnCommand::_process_OT_BOTH()
{
  using T = typename datatypes::WidthToSIntegralType<W>::type;
  bpp->ridCount = outMsg->NVALS;
  bpp->ridMap = outMsg->RidFlags;
  uint8_t* outPtr = reinterpret_cast<uint8_t*>(&outMsg[1]);
  auto* ridPos = primitives::getRIDArrayPosition(outPtr, 0);
  T* valuesPos = primitives::getValuesArrayPosition<T>(primitives::getFirstValueArrayPosition(outMsg), 0);

  for (size_t i = 0; i < outMsg->NVALS; ++i)
  {
    bpp->relRids[i] = ridPos[i];
    values[i] = valuesPos[i];
  }
}

template <>
void ColumnCommand::_process_OT_BOTH<16>()
{
  using T = typename datatypes::WidthToSIntegralType<16>::type;
  bpp->ridCount = outMsg->NVALS;
  bpp->ridMap = outMsg->RidFlags;
  uint8_t* outPtr = reinterpret_cast<uint8_t*>(&outMsg[1]);
  auto* ridPos = primitives::getRIDArrayPosition(outPtr, 0);
  T* valuesPos = primitives::getValuesArrayPosition<T>(primitives::getFirstValueArrayPosition(outMsg), 0);

  for (size_t i = 0; i < outMsg->NVALS; ++i)
  {
    bpp->relRids[i] = ridPos[i];
    datatypes::TSInt128::assignPtrPtr(&wide128Values[i], &valuesPos[i]);
  }
}

void ColumnCommand::process_OT_BOTH()
{
  if (makeAbsRids)
  {
    switch (colType.colWidth)
    {
      case 16: _process_OT_BOTH_wAbsRids<16>(); break;
      case 8: _process_OT_BOTH_wAbsRids<8>(); break;
      case 4: _process_OT_BOTH_wAbsRids<4>(); break;
      case 2: _process_OT_BOTH_wAbsRids<2>(); break;
      case 1: _process_OT_BOTH_wAbsRids<1>(); break;
      default:
        throw NotImplementedExcept(
            std::string("ColumnCommand::process_OT_BOTH with ABS RIDs does not support ") +
            std::to_string(colType.colWidth) + std::string(" byte width."));
    }
  }
  else
  {
    switch (colType.colWidth)
    {
      case 16: _process_OT_BOTH<16>(); break;
      case 8: _process_OT_BOTH<8>(); break;
      case 4: _process_OT_BOTH<4>(); break;
      case 2: _process_OT_BOTH<2>(); break;
      case 1: _process_OT_BOTH<1>(); break;
      default:
        throw NotImplementedExcept(std::string("ColumnCommand::process_OT_BOTH does not support ") +
                                   std::to_string(colType.colWidth) + std::string(" byte width."));
    }
  }
}

void ColumnCommand::process_OT_RID()
{
  memcpy(bpp->relRids, outMsg + 1, outMsg->NVALS << 1);
  bpp->ridCount = outMsg->NVALS;
  bpp->ridMap = outMsg->RidFlags;
}

// TODO This spec makes an impicit type conversion to fit types with sizeof(type) <= 4
// into 8 bytes. Treat values as a buffer and use memcpy to store the values in one go.
template <int W>
void ColumnCommand::_process_OT_DATAVALUE()
{
  using T = typename datatypes::WidthToSIntegralType<W>::type;
  bpp->ridCount = outMsg->NVALS;
  T* arr = (T*)(outMsg + 1);
  for (size_t i = 0; i < outMsg->NVALS; ++i)
    values[i] = arr[i];
}

template <>
void ColumnCommand::_process_OT_DATAVALUE<8>()
{
  bpp->ridCount = outMsg->NVALS;
  memcpy(values, outMsg + 1, outMsg->NVALS << 3);
}

template <>
void ColumnCommand::_process_OT_DATAVALUE<16>()
{
  bpp->ridCount = outMsg->NVALS;
  memcpy(wide128Values, outMsg + 1, outMsg->NVALS << 4);
}

void ColumnCommand::process_OT_DATAVALUE()
{
  bpp->ridCount = outMsg->NVALS;

  switch (colType.colWidth)
  {
    case 16: _process_OT_DATAVALUE<16>(); break;

    case 8: _process_OT_DATAVALUE<8>(); break;

    case 4: _process_OT_DATAVALUE<4>(); break;

    case 2: _process_OT_DATAVALUE<2>(); break;

    case 1: _process_OT_DATAVALUE<2>(); break;

    default:
      throw NotImplementedExcept(std::string("ColumnCommand::process_OT_DATAVALUE does not support ") +
                                 std::to_string(colType.colWidth) + std::string(" byte width."));
  }
}

void ColumnCommand::processResult()
{
  /* Switch on output type, turn pCol output into something useful, store it in
     the containing BPP */

  switch (outMsg->OutputType)
  {
    case OT_BOTH: process_OT_BOTH(); break;

    case OT_RID: process_OT_RID(); break;

    case OT_DATAVALUE: process_OT_DATAVALUE(); break;

    default:
      cout << "outputType = " << outMsg->OutputType << endl;
      throw logic_error("ColumnCommand got a bad OutputType");
  }

  // check if feeding a filtercommand
  if (fFilterFeeder == LEFT_FEEDER)
  {
    bpp->fFiltRidCount[0] = bpp->ridCount;

    for (uint64_t i = 0; i < bpp->ridCount; i++)
      bpp->fFiltCmdRids[0][i] = bpp->relRids[i];
  }
  else if (fFilterFeeder == RIGHT_FEEDER)
  {
    bpp->fFiltRidCount[1] = bpp->ridCount;

    for (uint64_t i = 0; i < bpp->ridCount; i++)
      bpp->fFiltCmdRids[1][i] = bpp->relRids[i];
  }
}

// Used by PseudoCC at least
void ColumnCommand::createCommand(ByteStream& bs)
{
  uint8_t tmp8;
  bs.advance(1);
  colType.unserialize(bs);
  bs >> tmp8;
  _isScan = tmp8;
  bs >> traceFlags;
  bs >> filterString;
  bs >> BOP;
  bs >> filterCount;
  deserializeInlineVector(bs, lastLbid);

  Command::createCommand(bs);
  switch (colType.colWidth)
  {
    case 1: createColumnFilter<datatypes::WidthToSIntegralType<1>::type>(); break;

    case 2: createColumnFilter<datatypes::WidthToSIntegralType<2>::type>(); break;

    case 4: createColumnFilter<datatypes::WidthToSIntegralType<4>::type>(); break;

    case 8: createColumnFilter<datatypes::WidthToSIntegralType<8>::type>(); break;

    case 16: createColumnFilter<datatypes::WidthToSIntegralType<16>::type>(); break;

    default:
      throw NotImplementedExcept(std::string("ColumnCommand::createCommand does not support ") +
                                 std::to_string(colType.colWidth) + std::string(" byte width."));
  }
}

void ColumnCommand::createCommand(execplan::CalpontSystemCatalog::ColType& aColType, ByteStream& bs)
{
  colType = aColType;
  uint8_t tmp8;

  bs >> tmp8;
  _isScan = tmp8;
  bs >> traceFlags;
  bs >> filterString;
  bs >> BOP;
  bs >> filterCount;
  deserializeInlineVector(bs, lastLbid);

  Command::createCommand(bs);
}

void ColumnCommand::resetCommand(ByteStream& bs)
{
  bs >> lbid;
}

void ColumnCommand::prep(int8_t outputType, bool absRids)
{
  fillInPrimitiveMessageHeader(outputType, absRids);

  switch (colType.colWidth)
  {
    case 1:
      shift = 16;
      mask = 0xFFFF;
      break;

    case 2:
      shift = 8;
      mask = 0xFF;
      break;

    case 4:
      shift = 4;
      mask = 0x0F;
      break;

    case 8:
      shift = 2;
      mask = 0x03;
      break;

    case 16:
      shift = 1;
      mask = 0x01;
      break;

    default:
      cout << "CC: colWidth is " << colType.colWidth << endl;
      throw logic_error("ColumnCommand: bad column width?");
  }
}

void ColumnCommand::fillInPrimitiveMessageHeader(const int8_t outputType, const bool absRids)
{
  // WIP Align this structure or move input RIDs away.
  baseMsgLength = sizeof(NewColRequestHeader) + (suppressFilter ? 0 : filterString.length());
  size_t inputMsgBufSize = baseMsgLength + (LOGICAL_BLOCK_RIDS * sizeof(primitives::RIDType));

  if (!inputMsg)
    inputMsg.reset(reinterpret_cast<uint8_t*>(aligned_alloc(utils::MAXCOLUMNWIDTH, inputMsgBufSize)));

  primMsg = (NewColRequestHeader*)inputMsg.get();
  outMsg = (ColResultHeader*)bpp->outputMsg.get();
  makeAbsRids = absRids;
  primMsg->ism.Interleave = 0;
  primMsg->ism.Flags = 0;
  primMsg->ism.Command = COL_BY_SCAN;
  primMsg->ism.Size = sizeof(NewColRequestHeader) + (suppressFilter ? 0 : filterString.length());
  primMsg->ism.Type = 2;
  primMsg->hdr.SessionID = bpp->sessionID;
  primMsg->hdr.TransactionID = bpp->txnID;
  primMsg->hdr.VerID = bpp->versionInfo.currentScn;
  primMsg->hdr.StepID = bpp->stepID;
  primMsg->colType = ColRequestHeaderDataType(colType);
  primMsg->OutputType = outputType;
  primMsg->BOP = BOP;
  primMsg->NOPS = (suppressFilter ? 0 : filterCount);
  primMsg->sort = 0;
}

/* Assumes OT_DATAVALUE */
void ColumnCommand::projectResult()
{
  auto nvals = outMsg->NVALS;
  if (primMsg->NVALS != nvals || nvals != bpp->ridCount)
  {
    ostringstream os;
    BRM::DBRM brm;
    BRM::OID_t oid;
    uint16_t l_dbroot;
    uint32_t partNum;
    uint16_t segNum;
    uint32_t fbo;
    brm.lookupLocal(lbid, 0, false, oid, l_dbroot, partNum, segNum, fbo);

    os << __FILE__ << " error on projection for oid " << oid << " lbid " << lbid;

    if (primMsg->NVALS != outMsg->NVALS)
      os << ": input rids " << primMsg->NVALS;
    else
      os << ": ridcount " << bpp->ridCount;

    os << ", output rids " << nvals << endl;

    // cout << os.str();
    if (bpp->sessionID & 0x80000000)
      throw NeedToRestartJob(os.str());
    else
      throw PrimitiveColumnProjectResultExcept(os.str());
  }

  idbassert(primMsg->NVALS == nvals);
  idbassert(bpp->ridCount == nvals);
  uint32_t valuesByteSize = nvals * colType.colWidth;
  *bpp->serialized << valuesByteSize;
  bpp->serialized->append(primitives::getFirstValueArrayPosition(outMsg), valuesByteSize);
}

void ColumnCommand::removeRowsFromRowGroup(RowGroup& rg)
{
  uint32_t gapSize = colType.colWidth + 2;
  uint8_t* msg8;
  uint16_t rid;
  Row oldRow, newRow;
  uint32_t oldIdx, newIdx;

  rg.initRow(&oldRow);
  rg.initRow(&newRow);
  rg.getRow(0, &oldRow);
  rg.getRow(0, &newRow);
  msg8 = (uint8_t*)(outMsg + 1);

  for (oldIdx = newIdx = 0; newIdx < outMsg->NVALS; newIdx++, msg8 += gapSize)
  {
    rid = *((uint16_t*)msg8);

    while (rid != bpp->relRids[oldIdx])
    {
      // need to find rid in relrids, and it is in there
      oldIdx++;
      oldRow.nextRow();
    }

    if (oldIdx != newIdx)
    {
      bpp->relRids[newIdx] = rid;
      // we use a memcpy here instead of copyRow() to avoid expanding the string table;
      memcpy(newRow.getData(), oldRow.getData(), newRow.getSize());
    }

    oldIdx++;
    oldRow.nextRow();
    newRow.nextRow();
  }

  rg.setRowCount(outMsg->NVALS);  // this gets rid of trailing rows, no need to set them to NULL
  bpp->ridCount = outMsg->NVALS;
  primMsg->NVALS = outMsg->NVALS;
}

template <typename T>
void ColumnCommand::_projectResultRGLoop(rowgroup::Row& r, const T* valuesArray, const uint32_t offset)
{
  for (size_t i = 0; i < outMsg->NVALS; ++i)
  {
    r.setIntField_offset(valuesArray[i], offset);
    r.nextRow(rowSize);
  }
}

template <>
void ColumnCommand::_projectResultRGLoop<int128_t>(rowgroup::Row& r, const int128_t* valuesArray,
                                                   const uint32_t offset)
{
  for (size_t i = 0; i < outMsg->NVALS; ++i)
  {
    r.setBinaryField_offset(&valuesArray[i], colType.colWidth, offset);
    r.nextRow(rowSize);
  }
}

template <int W>
void ColumnCommand::_projectResultRG(RowGroup& rg, uint32_t pos)
{
  using T = typename datatypes::WidthToSIntegralType<W>::type;
  T* valuesArray = primitives::getValuesArrayPosition<T>(primitives::getFirstValueArrayPosition(outMsg), 0);
  uint32_t offset;
  auto nvals = outMsg->NVALS;

  rg.initRow(&r);
  offset = r.getOffset(pos);
  rowSize = r.getSize();

  if ((primMsg->NVALS != nvals || nvals != bpp->ridCount) && (!noVB || bpp->sessionID & 0x80000000))
  {
    ostringstream os;
    BRM::DBRM brm;
    BRM::OID_t oid;
    uint16_t dbroot;
    uint32_t partNum;
    uint16_t segNum;
    uint32_t fbo;
    brm.lookupLocal(lbid, 0, false, oid, dbroot, partNum, segNum, fbo);

    os << __FILE__ << " error on projectResultRG for oid " << oid << " lbid " << lbid;

    if (primMsg->NVALS != nvals)
      os << ": input rids " << primMsg->NVALS;
    else
      os << ": ridcount " << bpp->ridCount;

    os << ",  output rids " << nvals;

    os << endl;

    if (bpp->sessionID & 0x80000000)
      throw NeedToRestartJob(os.str());
    else
      throw PrimitiveColumnProjectResultExcept(os.str());
  }
  else if (primMsg->NVALS != nvals || nvals != bpp->ridCount)
    removeRowsFromRowGroup(rg);

  idbassert(primMsg->NVALS == nvals);
  idbassert(bpp->ridCount == nvals);
  rg.getRow(0, &r);

  _projectResultRGLoop(r, valuesArray, offset);
}

void ColumnCommand::projectResultRG(RowGroup& rg, uint32_t pos)
{
  switch (colType.colWidth)
  {
    case 1: _projectResultRG<1>(rg, pos); break;
    case 2: _projectResultRG<2>(rg, pos); break;
    case 4: _projectResultRG<4>(rg, pos); break;
    case 8: _projectResultRG<8>(rg, pos); break;
    case 16: _projectResultRG<16>(rg, pos); break;
  }
}

void ColumnCommand::project()
{
  /* bpp->ridCount == 0 would signify a scan operation */
  if (bpp->ridCount == 0)
  {
    *bpp->serialized << (uint32_t)0;
    blockCount += colType.colWidth;
    return;
  }

  makeStepMsg();
  issuePrimitive();
  projectResult();
}

void ColumnCommand::projectIntoRowGroup(RowGroup& rg, uint32_t pos)
{
  if (bpp->ridCount == 0)
  {
    blockCount += colType.colWidth;
    return;
  }

  makeStepMsg();
  issuePrimitive();
  projectResultRG(rg, pos);
}

void ColumnCommand::nextLBID()
{
  lbid += colType.colWidth;
}

void ColumnCommand::duplicate(ColumnCommand* cc)
{
  cc->_isScan = _isScan;
  cc->traceFlags = traceFlags;
  cc->filterString = filterString;
  cc->colType.colDataType = colType.colDataType;
  cc->colType.compressionType = colType.compressionType;
  cc->colType.colWidth = colType.colWidth;
  cc->colType.charsetNumber = colType.charsetNumber;
  cc->BOP = BOP;
  cc->filterCount = filterCount;
  cc->fFilterFeeder = fFilterFeeder;
  cc->parsedColumnFilter = parsedColumnFilter;
  cc->suppressFilter = suppressFilter;
  cc->lastLbid = lastLbid;
  cc->r = r;
  cc->rowSize = rowSize;
  cc->Command::operator=(*this);
}

SCommand ColumnCommand::duplicate()
{
  SCommand ret;

  ret.reset(new ColumnCommand());
  duplicate((ColumnCommand*)ret.get());
  return ret;
}

bool ColumnCommand::operator==(const ColumnCommand& cc) const
{
  if (_isScan != cc._isScan)
    return false;

  if (BOP != cc.BOP)
    return false;

  if (filterString != cc.filterString)
    return false;

  if (filterCount != cc.filterCount)
    return false;

  if (makeAbsRids != cc.makeAbsRids)
    return false;

  if (colType.colWidth != cc.colType.colWidth)
    return false;

  if (colType.colDataType != cc.colType.colDataType)
    return false;

  return true;
}

bool ColumnCommand::operator!=(const ColumnCommand& cc) const
{
  return !(*this == cc);
}

ColumnCommand& ColumnCommand::operator=(const ColumnCommand& c)
{
  _isScan = c._isScan;
  traceFlags = c.traceFlags;
  filterString = c.filterString;
  colType.colDataType = c.colType.colDataType;
  colType.compressionType = c.colType.compressionType;
  colType.colWidth = c.colType.colWidth;
  BOP = c.BOP;
  filterCount = c.filterCount;
  fFilterFeeder = c.fFilterFeeder;
  parsedColumnFilter = c.parsedColumnFilter;
  suppressFilter = c.suppressFilter;
  lastLbid = c.lastLbid;
  return *this;
}

bool ColumnCommand::willPrefetch()
{
  return (blockCount == 0 || ((double)loadCount) / ((double)blockCount) > bpp->prefetchThreshold);
}

void ColumnCommand::disableFilters()
{
  suppressFilter = true;
  prep(primMsg->OutputType, makeAbsRids);
}

void ColumnCommand::enableFilters()
{
  suppressFilter = false;
  prep(primMsg->OutputType, makeAbsRids);
}

void ColumnCommand::getLBIDList(uint32_t loopCount, vector<int64_t>* lbids)
{
  int64_t firstLBID = lbid, lastLBID = firstLBID + (loopCount * colType.colWidth) - 1, i;

  for (i = firstLBID; i <= lastLBID; i++)
    lbids->push_back(i);
}

int64_t ColumnCommand::getLastLbid()
{
  if (!_isScan)
    return 0;

  return lastLbid[bpp->dbRoot - 1];
}

ColumnCommand* ColumnCommandFabric::createCommand(messageqcpp::ByteStream& bs)
{
  bs.advance(1);  // The higher dispatcher Command::makeCommand calls BS::peek so this increments BS ptr
  execplan::CalpontSystemCatalog::ColType colType;
  colType.unserialize(bs);
  switch (colType.colWidth)
  {
    case 1: return new ColumnCommandInt8(colType, bs); break;

    case 2: return new ColumnCommandInt16(colType, bs); break;

    case 4: return new ColumnCommandInt32(colType, bs); break;

    case 8: return new ColumnCommandInt64(colType, bs); break;

    case 16: return new ColumnCommandInt128(colType, bs); break;

    default:
      throw NotImplementedExcept(std::string("ColumnCommandFabric::createCommand: unsupported width ") +
                                 std::to_string(colType.colWidth));
  }

  return nullptr;
}

template <typename T>
void ColumnCommand::createColumnFilter()
{
  parsedColumnFilter =
      primitives::_parseColumnFilter<T>(filterString.buf(), colType.colDataType, filterCount, BOP);
  /* OR hack */
  emptyFilter = primitives::_parseColumnFilter<T>(filterString.buf(), colType.colDataType, 0, BOP);
}

ColumnCommand* ColumnCommandFabric::duplicate(const ColumnCommandUniquePtr& rhs)
{
  auto& command = *rhs;
  if (LIKELY(typeid(command) == typeid(ColumnCommandInt64)))
  {
    ColumnCommandInt64* ret = new ColumnCommandInt64();
    *ret = *dynamic_cast<ColumnCommandInt64*>(rhs.get());
    return ret;
  }
  else if (typeid(command) == typeid(ColumnCommandInt128))
  {
    ColumnCommandInt128* ret = new ColumnCommandInt128();
    *ret = *dynamic_cast<ColumnCommandInt128*>(rhs.get());
    return ret;
  }
  else if (typeid(command) == typeid(ColumnCommandInt8))
  {
    ColumnCommandInt8* ret = new ColumnCommandInt8();
    *ret = *dynamic_cast<ColumnCommandInt8*>(rhs.get());
    return ret;
  }
  else if (typeid(command) == typeid(ColumnCommandInt16))
  {
    ColumnCommandInt16* ret = new ColumnCommandInt16();
    *ret = *dynamic_cast<ColumnCommandInt16*>(rhs.get());
    return ret;
  }
  else if (typeid(command) == typeid(ColumnCommandInt32))
  {
    ColumnCommandInt32* ret = new ColumnCommandInt32();
    *ret = *dynamic_cast<ColumnCommandInt32*>(rhs.get());
    return ret;
  }
  else
    throw NotImplementedExcept("ColumnCommandFabric::duplicate: Can not detect ColumnCommand child class");

  return nullptr;
}

// Code duplication here for the future patch.
ColumnCommandInt8::ColumnCommandInt8(execplan::CalpontSystemCatalog::ColType& aColType,
                                     messageqcpp::ByteStream& bs)
{
  ColumnCommand::createCommand(aColType, bs);
  createColumnFilter<IntegralType>();
}

void ColumnCommandInt8::prep(int8_t outputType, bool absRids)
{
  shift = 16;
  mask = 0xFFFF;
  ColumnCommand::fillInPrimitiveMessageHeader(outputType, absRids);
}

void ColumnCommandInt8::loadData()
{
  _loadData<size>();
}

void ColumnCommandInt8::process_OT_BOTH()
{
  if (makeAbsRids)
    _process_OT_BOTH_wAbsRids<size>();
  else
    _process_OT_BOTH<size>();
}

void ColumnCommandInt8::process_OT_DATAVALUE()
{
  _process_OT_DATAVALUE<size>();
}

void ColumnCommandInt8::projectResultRG(RowGroup& rg, uint32_t pos)
{
  _projectResultRG<size>(rg, pos);
}

void ColumnCommandInt8::issuePrimitive()
{
  _issuePrimitiveNarrow<size>();
}

ColumnCommandInt16::ColumnCommandInt16(execplan::CalpontSystemCatalog::ColType& aColType,
                                       messageqcpp::ByteStream& bs)
{
  ColumnCommand::createCommand(aColType, bs);
  createColumnFilter<IntegralType>();
}

void ColumnCommandInt16::prep(int8_t outputType, bool absRids)
{
  shift = 8;
  mask = 0xFF;
  ColumnCommand::fillInPrimitiveMessageHeader(outputType, absRids);
}

void ColumnCommandInt16::loadData()
{
  _loadData<size>();
}

void ColumnCommandInt16::process_OT_BOTH()
{
  if (makeAbsRids)
    _process_OT_BOTH_wAbsRids<size>();
  else
    _process_OT_BOTH<size>();
}

void ColumnCommandInt16::process_OT_DATAVALUE()
{
  _process_OT_DATAVALUE<size>();
}

void ColumnCommandInt16::projectResultRG(RowGroup& rg, uint32_t pos)
{
  _projectResultRG<size>(rg, pos);
}

void ColumnCommandInt16::issuePrimitive()
{
  _issuePrimitiveNarrow<size>();
}

ColumnCommandInt32::ColumnCommandInt32(execplan::CalpontSystemCatalog::ColType& aColType,
                                       messageqcpp::ByteStream& bs)
{
  ColumnCommand::createCommand(aColType, bs);
  createColumnFilter<IntegralType>();
}

void ColumnCommandInt32::prep(int8_t outputType, bool absRids)
{
  shift = 4;
  mask = 0x0F;
  ColumnCommand::fillInPrimitiveMessageHeader(outputType, absRids);
}

void ColumnCommandInt32::loadData()
{
  _loadData<size>();
}

void ColumnCommandInt32::process_OT_BOTH()
{
  if (makeAbsRids)
    _process_OT_BOTH_wAbsRids<size>();
  else
    _process_OT_BOTH<size>();
}

void ColumnCommandInt32::process_OT_DATAVALUE()
{
  _process_OT_DATAVALUE<size>();
}

void ColumnCommandInt32::projectResultRG(RowGroup& rg, uint32_t pos)
{
  _projectResultRG<size>(rg, pos);
}

void ColumnCommandInt32::issuePrimitive()
{
  _issuePrimitiveNarrow<size>();
}

ColumnCommandInt64::ColumnCommandInt64(execplan::CalpontSystemCatalog::ColType& aColType,
                                       messageqcpp::ByteStream& bs)
{
  ColumnCommand::createCommand(aColType, bs);
  createColumnFilter<IntegralType>();
}

void ColumnCommandInt64::prep(int8_t outputType, bool absRids)
{
  shift = 2;
  mask = 0x03;
  ColumnCommand::fillInPrimitiveMessageHeader(outputType, absRids);
}

void ColumnCommandInt64::loadData()
{
  _loadData<size>();
}

void ColumnCommandInt64::process_OT_BOTH()
{
  if (makeAbsRids)
    _process_OT_BOTH_wAbsRids<size>();
  else
    _process_OT_BOTH<size>();
}

void ColumnCommandInt64::process_OT_DATAVALUE()
{
  _process_OT_DATAVALUE<size>();
}

void ColumnCommandInt64::projectResultRG(RowGroup& rg, uint32_t pos)
{
  _projectResultRG<size>(rg, pos);
}

void ColumnCommandInt64::issuePrimitive()
{
  _issuePrimitiveNarrow<size>();
}

ColumnCommandInt128::ColumnCommandInt128(execplan::CalpontSystemCatalog::ColType& aColType,
                                         messageqcpp::ByteStream& bs)
{
  ColumnCommand::createCommand(aColType, bs);
  createColumnFilter<IntegralType>();
}

void ColumnCommandInt128::prep(int8_t outputType, bool absRids)
{
  shift = 1;
  mask = 0x01;
  ColumnCommand::fillInPrimitiveMessageHeader(outputType, absRids);
}

void ColumnCommandInt128::loadData()
{
  _loadData<size>();
}

void ColumnCommandInt128::process_OT_BOTH()
{
  if (makeAbsRids)
    _process_OT_BOTH_wAbsRids<size>();
  else
    _process_OT_BOTH<size>();
}

void ColumnCommandInt128::process_OT_DATAVALUE()
{
  _process_OT_DATAVALUE<size>();
}

void ColumnCommandInt128::projectResultRG(RowGroup& rg, uint32_t pos)
{
  _projectResultRG<size>(rg, pos);
}

void ColumnCommandInt128::issuePrimitive()
{
  loadData();

  if (!suppressFilter)
    bpp->getPrimitiveProcessor().setParsedColumnFilter(parsedColumnFilter);
  else
    bpp->getPrimitiveProcessor().setParsedColumnFilter(emptyFilter);

  _issuePrimitive<size>();
  if (_isScan)
    updateCPDataWide();
}

}  // namespace primitiveprocessor
// vim:ts=4 sw=4:
