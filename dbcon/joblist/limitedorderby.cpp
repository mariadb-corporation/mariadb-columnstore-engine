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

//  $Id: limitedorderby.cpp 9581 2013-05-31 13:46:14Z pleblanc $

#include <iostream>
// #define NDEBUG
#include <cassert>
#include <string>
using namespace std;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "rowgroup.h"
using namespace rowgroup;

#include "jlf_common.h"
#include "limitedorderby.h"

using namespace ordering;

namespace joblist
{
const uint64_t LimitedOrderBy::fMaxUncommited = 102400;  // 100KiB - make it configurable?

// LimitedOrderBy class implementation
LimitedOrderBy::LimitedOrderBy(ResourceManager* rm)
 : DiskBasedTopNOrderBy(rm), fStart(0), fCount(-1), fUncommitedMemory(0)
{
  fRule.fIdbCompare = this;
}

LimitedOrderBy::~LimitedOrderBy()
{
}

void LimitedOrderBy::initialize(const RowGroup& rg, const JobInfo& jobInfo, bool invertRules,
                                bool isMultiThreaded)
{
  fRm = jobInfo.rm;
  fSessionMemLimit = jobInfo.umMemLimit;
  fErrorCode = ERR_ORDERBY_TOO_BIG;

  // locate column position in the rowgroup
  map<uint32_t, uint32_t> keyToIndexMap;

  for (uint64_t i = 0; i < rg.getKeys().size(); ++i)
  {
    if (keyToIndexMap.find(rg.getKeys()[i]) == keyToIndexMap.end())
      keyToIndexMap.insert(make_pair(rg.getKeys()[i], i));
  }

  vector<pair<uint32_t, bool> >::const_iterator i = jobInfo.orderByColVec.begin();

  for (; i != jobInfo.orderByColVec.end(); i++)
  {
    map<uint32_t, uint32_t>::iterator j = keyToIndexMap.find(i->first);
    idbassert(j != keyToIndexMap.end());

    fOrderByCond.push_back(IdbSortSpec(j->second, i->second ^ invertRules));
  }

  // limit row count info
  if (isMultiThreaded)
  {
    // CS can't apply offset at the first stage
    // otherwise it looses records.
    fStart = 0;
    fCount = jobInfo.limitStart + jobInfo.limitCount;
  }
  else
  {
    fStart = jobInfo.limitStart;
    fCount = jobInfo.limitCount;
  }

  IdbOrderBy::initialize(rg);
}

// This must return a proper number of key columns and
// not just a column count.
uint64_t LimitedOrderBy::getKeyLength() const
{
  return fRow0.getColumnCount();
}

void LimitedOrderBy::processRow(const rowgroup::Row& row)
{
  // check if this is a distinct row
  if (fDistinct && fDistinctMap->find(row.getPointer()) != fDistinctMap->end())
    return;

  // @bug5312, limit count is 0, do nothing.
  if (fCount == 0)
    return;

  // std::cout << "LimitedOrderBy::processRow row " << row.toString() << std::endl;
  // std::cout << "LimitedOrderBy::processRow fStart " << fStart << " fCount " << fCount << std::endl;
  auto& orderedRowsQueue = getQueue();
  // if the row count is less than the limit
  if (orderedRowsQueue.size() < fStart + fCount)
  {
    copyRow(row, &fRow0);
    OrderByRow newRow(fRow0, fRule);
    orderedRowsQueue.push(newRow);

    uint64_t memSizeInc = sizeof(newRow);
    fUncommitedMemory += memSizeInc;
    if (fUncommitedMemory >= fMaxUncommited)
    {
      if (!fRm->getMemory(fUncommitedMemory, fSessionMemLimit))
      {
        cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode) << " @" << __FILE__ << ":" << __LINE__;
        throw IDBExcept(fErrorCode);
      }
      fMemSize += fUncommitedMemory;
      fUncommitedMemory = 0;
    }

    // add to the distinct map
    if (fDistinct)
      fDistinctMap->insert(fRow0.getPointer());

    fRowGroup.incRowCount();
    fRow0.nextRow();

    if (fRowGroup.getRowCount() >= fRowsPerRG)
    {
      fDataQueue.push(fData);
      uint64_t newSize = fRowGroup.getSizeWithStrings() - fRowGroup.getHeaderSize();

      if (!fRm->getMemory(newSize, fSessionMemLimit))
      {
        cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode) << " @" << __FILE__ << ":" << __LINE__;
        throw IDBExcept(fErrorCode);
      }
      fMemSize += newSize;

      fData.reinit(fRowGroup, fRowsPerRG);
      fRowGroup.setData(&fData);
      fRowGroup.resetRowGroup(0);
      fRowGroup.getRow(0, &fRow0);
    }
  }
  else if (fOrderByCond.size() > 0 && fRule.less(row.getPointer(), orderedRowsQueue.top().fData))
  {
    OrderByRow swapRow = orderedRowsQueue.top();
    row1.setData(swapRow.fData);
    // std::cout << "LimitedOrderBy::processRow row2swap " << row1.toString() << std::endl;
    // std::cout << "LimitedOrderBy::processRow new row 4 swaping " << row.toString() << std::endl;

    copyRow(row, &row1);

    if (fDistinct)
    {
      fDistinctMap->erase(orderedRowsQueue.top().fData);
      fDistinctMap->insert(row1.getPointer());
    }

    orderedRowsQueue.pop();
    orderedRowsQueue.push(swapRow);
  }
}

// void LimitedOrderBy::brandNewFinalize()
// {
//   auto& orderedRowsQueue = getQueue();

//   // Skip OFFSET
//   uint64_t sqlOffset = fStart;
//   std::cout << "brandNewFinalize offset " << sqlOffset << " orderedRowsQueue.size() " <<
//   orderedRowsQueue.size() << std::endl; while (sqlOffset > 0 && !orderedRowsQueue.empty())
//   {
//     auto r = orderedRowsQueue.top();
//     row1.setData(r.fData);
//     std::cout << "brandNewFinalize row " << row1.toString() << std::endl;
//     orderedRowsQueue.pop();
//     --sqlOffset;
//   }
// }

void LimitedOrderBy::flushCurrentToDisk_(const bool firstFlush)
{
  // make a queue with rgdatas and hand it to DiskBasedTopNOrderBy
  auto dl = RowGroupDL(1, 1);
  auto& orderedRowsQueue = getQueue();
  size_t rowsOverRG = orderedRowsQueue.size() % fRowsPerRG;
  size_t numberOfRGs = orderedRowsQueue.size() / fRowsPerRG + static_cast<size_t>(rowsOverRG > 0);
  std::thread flushThread(&DiskBasedTopNOrderBy::flushCurrentToDisk, this, std::ref(dl), fRowGroup,
                          numberOfRGs, firstFlush);

  uint32_t rSize = fRow0.getSize();
  // process leftovers
  if (rowsOverRG)
  {
    fData.reinit(fRowGroup, rowsOverRG);
    fRowGroup.setData(&fData);
    fRowGroup.resetRowGroup(0);
    fRowGroup.getRow(rowsOverRG-1, &fRow0);
    
    const OrderByRow& topRow = orderedRowsQueue.top();
    row1.setData(topRow.fData);
    copyRow(row1, &fRow0);
    fRowGroup.incRowCount();
    fRow0.prevRow(rSize);
    orderedRowsQueue.pop();

    dl.insert(fData);
  }

  if (orderedRowsQueue.size() > 0)
  {
    fData.reinit(fRowGroup, fRowsPerRG);
    fRowGroup.setData(&fData);
    fRowGroup.resetRowGroup(0);
    fRowGroup.getRow(fRowsPerRG-1, &fRow0);

    while (!orderedRowsQueue.empty())
    {
      const OrderByRow& topRow = orderedRowsQueue.top();
      row1.setData(topRow.fData);
      copyRow(row1, &fRow0);
      fRowGroup.incRowCount();
      fRow0.prevRow(rSize);
      orderedRowsQueue.pop();

      if (fRowGroup.getRowCount() == fRowsPerRG)
      {
        dl.insert(fData);

        fData.reinit(fRowGroup, fRowsPerRG);
        fRowGroup.setData(&fData);
        fRowGroup.resetRowGroup(0);
        fRowGroup.getRow(fRowsPerRG-1, &fRow0);
      }
    }

    if (fRowGroup.getRowCount() > 0)
      dl.insert(fData);
  }

  dl.endOfInput();

  // clean up the current queue/rgdatas to free mem
  // fDataQueue
  // fDistinctMap
  // orderedRowsQueue
  queue<RGData> tempQueue;
  fDataQueue.swap(tempQueue);
  if (fDistinctMap)
  {
    fDistinctMap->clear();
  }

  flushThread.join();
}

void LimitedOrderBy::brandNewFinalize()
{
  if (!isDiskBased())
  {
    return finalize();
  }

  // if disk-based
  // here there are <= inputQueuesNumber files on disk
  // and potentially some in-memory state
  // need to merge this together to produce a result
}

/*
 * The f() copies top element from an ordered queue into a row group. It
 * does this backwards to syncronise sorting orientation with the server.
 * The top row from the queue goes last into the returned set.
 */
void LimitedOrderBy::finalize()
{
  if (fUncommitedMemory > 0)
  {
    if (!fRm->getMemory(fUncommitedMemory, fSessionMemLimit))
    {
      cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode) << " @" << __FILE__ << ":" << __LINE__;
      throw logging::OutOfMemoryExcept(fErrorCode);
    }
    fMemSize += fUncommitedMemory;
    fUncommitedMemory = 0;
  }

  queue<RGData> tempQueue;
  if (fRowGroup.getRowCount() > 0)
    fDataQueue.push(fData);

  auto& orderedRowsQueue = getQueue();

  if (orderedRowsQueue.size() > 0)
  {
    // *DRRTUY Very memory intensive. CS needs to account active
    // memory only and release memory if needed.
    uint64_t memSizeInc = fRowGroup.getSizeWithStrings() - fRowGroup.getHeaderSize();

    if (!fRm->getMemory(memSizeInc, fSessionMemLimit))
    {
      cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode) << " @" << __FILE__ << ":" << __LINE__;
      throw logging::OutOfMemoryExcept(fErrorCode);
    }
    fMemSize += memSizeInc;

    uint64_t offset = 0;
    uint64_t i = 0;
    // Reduce queue size by an offset value if it applicable.
    uint64_t queueSizeWoOffset = orderedRowsQueue.size() > fStart ? orderedRowsQueue.size() - fStart : 0;
    list<RGData> tempRGDataList;

    if (fCount <= queueSizeWoOffset)
    {
      offset = fCount % fRowsPerRG;
      if (!offset && fCount > 0)
        offset = fRowsPerRG;
    }
    else
    {
      offset = queueSizeWoOffset % fRowsPerRG;
      if (!offset && queueSizeWoOffset > 0)
        offset = fRowsPerRG;
    }

    list<RGData>::iterator tempListIter = tempRGDataList.begin();

    i = 0;
    uint32_t rSize = fRow0.getSize();
    uint64_t preLastRowNumb = fRowsPerRG - 1;
    fData.reinit(fRowGroup, fRowsPerRG);
    fRowGroup.setData(&fData);
    fRowGroup.resetRowGroup(0);
    // *DRRTUY This approach won't work with
    // OFSET > fRowsPerRG
    offset = offset != 0 ? offset - 1 : offset;
    fRowGroup.getRow(offset, &fRow0);

    while ((orderedRowsQueue.size() > fStart) && (i++ < fCount))
    {
      const OrderByRow& topRow = orderedRowsQueue.top();
      row1.setData(topRow.fData);
      copyRow(row1, &fRow0);
      fRowGroup.incRowCount();
      offset--;
      fRow0.prevRow(rSize);
      orderedRowsQueue.pop();

      // if RG has fRowsPerRG rows
      if (offset == (uint64_t)-1)
      {
        tempRGDataList.push_front(fData);

        if (!fRm->getMemory(memSizeInc, fSessionMemLimit))
        {
          cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode) << " @" << __FILE__ << ":" << __LINE__;
          throw logging::OutOfMemoryExcept(fErrorCode);
        }
        fMemSize += memSizeInc;

        fData.reinit(fRowGroup, fRowsPerRG);
        fRowGroup.setData(&fData);
        fRowGroup.resetRowGroup(0);  // ?
        fRowGroup.getRow(preLastRowNumb, &fRow0);
        offset = preLastRowNumb;
      }
    }
    // Push the last/only group into the queue.
    if (fRowGroup.getRowCount() > 0)
      tempRGDataList.push_front(fData);

    for (tempListIter = tempRGDataList.begin(); tempListIter != tempRGDataList.end(); tempListIter++)
      tempQueue.push(*tempListIter);

    fDataQueue = tempQueue;
  }
}

// WIP UNUSED
bool LimitedOrderBy::getNextRGData(RGData& data)
{
  auto& orderedRowsQueue = getQueue();

  if (orderedRowsQueue.empty())
  {
    return false;
  }

  uint32_t rSize = fRow0.getSize();
  data.reinit(fRowGroup, fRowsPerRG);
  fRowGroup.setData(&data);
  fRowGroup.resetRowGroup(0);
  fRowGroup.getRow(0, &fRow0);

  uint64_t thisRGRowNumber = 0;
  // find number of rows to retrieve from the queue using SQL LIMIT
  // and the current sorted queue size.
  uint64_t rowsToRetrieve = std::min(fCount - fRowsReturned, fRowsPerRG);
  uint64_t rowsToRetrieveFromQueue = std::min(rowsToRetrieve, orderedRowsQueue.size());
  std::cout << "getNextRGData rowsToRetrieve " << rowsToRetrieve << " orderedRowsQueue.size() "
            << orderedRowsQueue.size() << std::endl;
  std::cout << "getNextRGData rowsToRetrieveFromQueue " << rowsToRetrieveFromQueue << std::endl;
  for (; rowsToRetrieveFromQueue > thisRGRowNumber; ++thisRGRowNumber)
  {
    const OrderByRow& topRow = orderedRowsQueue.top();
    row1.setData(topRow.fData);
    std::cout << "getNextRGData row " << row1.toString() << std::endl;
    copyRow(row1, &fRow0);
    fRowGroup.incRowCount();
    fRow0.nextRow(rSize);
    orderedRowsQueue.pop();
  }

  fRowsReturned += rowsToRetrieveFromQueue;

  return rowsToRetrieveFromQueue > 0;
}

const string LimitedOrderBy::toString() const
{
  ostringstream oss;
  oss << "OrderBy   cols: ";
  vector<IdbSortSpec>::const_iterator i = fOrderByCond.begin();

  for (; i != fOrderByCond.end(); i++)
    oss << "(" << i->fIndex << "," << ((i->fAsc) ? "Asc" : "Desc") << ","
        << ((i->fNf) ? "null first" : "null last") << ") ";

  oss << " start-" << fStart << " count-" << fCount;

  if (fDistinct)
    oss << " distinct";

  oss << endl;

  return oss.str();
}

}  // namespace joblist
