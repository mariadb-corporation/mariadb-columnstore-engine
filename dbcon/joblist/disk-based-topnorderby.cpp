/* Copyright (C) 2025 MariaDB Corp.

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

#include <vector>

#include "dumper.h"
#include "disk-based-topnorderby.h"
namespace joblist
{

// The caller ensures lifetime of dl and rg
void DiskBasedTopNOrderBy::flushCurrentToDisk(RowGroupDL& dl, rowgroup::RowGroup rg, const size_t numberOfRGs, const bool firstFlush)
{
  size_t rgid = (firstFlush) ? numberOfRGs : 0;
  rowgroup::RGData rgData;

  size_t generation = (firstFlush) ? getGenerationCounter() : 0; // WIP 

  bool more = dl.next(0, &rgData);
  while (more)
  {
    saveRG(rgid, generation, rg, &rgData);
    more = dl.next(0, &rgData);
    rgid = (firstFlush) ? rgid - 1 : rgid + 1;
  }

  if (firstFlush)
  {
    incrementGenerationCounter();
  }
  else
  {
    
  }
}
void DiskBasedTopNOrderBy::diskBasedMergePhaseIfNeeded(std::vector<RowGroupDLSPtr>& /*dataLists*/)
{
}

std::vector<std::string> DiskBasedTopNOrderBy::getGenerationFileNamesNextBatch(const size_t batchSize)
{
  // assert(getGenerationFilesNumber() > batchSize);
  auto totalNumberOfFilesYetToMerge = getGenerationFilesNumber() - batchSize;
  auto batchSizeOrFilesLeftNumber = std::max(getGenerationFilesNumber(), batchSize);
  auto actualBatchSize = std::min(totalNumberOfFilesYetToMerge, batchSizeOrFilesLeftNumber);
  // add state for the starting offset + wraparound
  size_t startOffset = 0;
  std::vector<std::string> res;
  res.reserve(actualBatchSize);
  for (size_t i = 0; i < startOffset + actualBatchSize; ++i)
  {
    res.push_back(makeRGFilePrefix(i));
  }

  return res;
 

}  // namespace joblist