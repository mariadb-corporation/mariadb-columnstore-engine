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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "dumper.h"
#include "elementtype.h"
#include "resourcemanager.h"
namespace joblist
{

class DiskBasedTopNOrderBy : public rowgroup::RGDumper
{
  // std::string fTmpDir =
  //     config::Config::makeConfig()->getTempFileDir(config::Config::TempDirPurpose::Aggregates);
  // std::string fCompStr = config::Config::makeConfig()->getConfig("RowAggregation", "Compression");
 public:
  // TODO Parametrize compression, tmpdir and memory manager (can be temp)
  DiskBasedTopNOrderBy(ResourceManager* rm)
   : RGDumper(compress::getCompressInterfaceByName("LZ4"), std::make_unique<rowgroup::MemManager>(),
              config::Config::makeConfig()->getTempFileDir(config::Config::TempDirPurpose::Sorting),
              "Sorting", reinterpret_cast<std::uintptr_t>(this))
  {
  }
  ~DiskBasedTopNOrderBy() = default;

  void incrementGenerationCounter()
  {
    ++fGenerationCounter;
  }
  uint64_t getGenerationCounter() const
  {
    return fGenerationCounter;
  }

  bool isDiskBased() const
  {
    return fGenerationCounter > 0;
  }

  size_t getGenerationFilesNumber() const
  {
    return 0;
  }
  std::vector<std::string> getGenerationFileNamesNextBatch(const size_t batchSize);

  // The caller ensures lifetime of dl and rg
  void flushCurrentToDisk(RowGroupDL& dl, rowgroup::RowGroup rg, const size_t numberOfRGs, const bool firstFlush);
  void diskBasedMergePhaseIfNeeded(std::vector<RowGroupDLSPtr>& dataLists);

  //  private:
  uint64_t fGenerationCounter{0};
};

}  // namespace joblist