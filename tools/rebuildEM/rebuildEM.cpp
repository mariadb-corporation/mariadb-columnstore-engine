/* Copyright (C) 2021 MariaDB Corporation

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

#include <iostream>
#include <stdint.h>

#include "rebuildEM.h"
#include "calpontsystemcatalog.h"
#include "idbcompress.h"
#include "blocksize.h"
#include "we_convertor.h"
#include "we_fileop.h"
#include "IDBPolicy.h"
#include "IDBFileSystem.h"
#include "we_chunkmanager.h"
#include "we_dctnrycompress.h"
#include "we_colopcompress.h"

using namespace idbdatafile;

namespace RebuildExtentMap
{
std::unordered_map<uint32_t, FileId> systemCatalogMap = {
    {2073, FileId(2073, 0, 0, 0, 0, execplan::CalpontSystemCatalog::VARCHAR, 201728, 0, true)},
    {2070, FileId(2070, 0, 0, 0, 0, execplan::CalpontSystemCatalog::VARCHAR, 163840, 0, true)},
    {2067, FileId(2067, 0, 0, 0, 0, execplan::CalpontSystemCatalog::VARCHAR, 114688, 0, true)},
    {2064, FileId(2064, 0, 0, 0, 0, execplan::CalpontSystemCatalog::VARCHAR, 98304, 0, true)},
    {2076, FileId(2076, 0, 0, 0, 0, execplan::CalpontSystemCatalog::VARCHAR, 218112, 0, true)},
    {2061, FileId(2061, 0, 0, 0, 0, execplan::CalpontSystemCatalog::VARCHAR, 81920, 0, true)},
    {1004, FileId(1004, 0, 0, 0, 4, execplan::CalpontSystemCatalog::DATE, 36864, 0, false)},
    {1022, FileId(1022, 0, 0, 0, 8, execplan::CalpontSystemCatalog::VARCHAR, 90112, 0, false)},
    {1001, FileId(1001, 0, 0, 0, 8, execplan::CalpontSystemCatalog::VARCHAR, 0, 0, false)},
    {1023, FileId(1023, 0, 0, 0, 8, execplan::CalpontSystemCatalog::VARCHAR, 106496, 0, false)},
    {1021, FileId(1021, 0, 0, 0, 8, execplan::CalpontSystemCatalog::VARCHAR, 73728, 0, false)},
    {1010, FileId(1010, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 61440, 0, false)},
    {1006, FileId(1006, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 45056, 0, false)},
    {1002, FileId(1002, 0, 0, 0, 8, execplan::CalpontSystemCatalog::VARCHAR, 16384, 0, false)},
    {1009, FileId(1009, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 57344, 0, false)},
    {1005, FileId(1005, 0, 0, 0, 4, execplan::CalpontSystemCatalog::DATE, 40960, 0, false)},
    {1011, FileId(1011, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 65536, 0, false)},
    {1012, FileId(1012, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 69632, 0, false)},
    {1008, FileId(1008, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 53248, 0, false)},
    {1007, FileId(1007, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 49152, 0, false)},
    {1003, FileId(1003, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 32768, 0, false)},
    {1032, FileId(1032, 0, 0, 0, 8, execplan::CalpontSystemCatalog::VARCHAR, 155648, 0, false)},
    {1038, FileId(1038, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 189440, 0, false)},
    {1033, FileId(1033, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 172032, 0, false)},
    {1027, FileId(1027, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 135168, 0, false)},
    {1024, FileId(1024, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 122880, 0, false)},
    {1042, FileId(1042, 0, 0, 0, 8, execplan::CalpontSystemCatalog::UBIGINT, 230400, 0, false)},
    {1040, FileId(1040, 0, 0, 0, 8, execplan::CalpontSystemCatalog::VARCHAR, 209920, 0, false)},
    {1025, FileId(1025, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 126976, 0, false)},
    {1035, FileId(1035, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 180224, 0, false)},
    {1028, FileId(1028, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 139264, 0, false)},
    {1036, FileId(1036, 0, 0, 0, 1, execplan::CalpontSystemCatalog::CHAR, 184320, 0, false)},
    {1031, FileId(1031, 0, 0, 0, 4, execplan::CalpontSystemCatalog::DATE, 151552, 0, false)},
    {1037, FileId(1037, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 185344, 0, false)},
    {1039, FileId(1039, 0, 0, 0, 8, execplan::CalpontSystemCatalog::VARCHAR, 193536, 0, false)},
    {1030, FileId(1030, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 147456, 0, false)},
    {1034, FileId(1034, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 176128, 0, false)},
    {1026, FileId(1026, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 131072, 0, false)},
    {1041, FileId(1041, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 226304, 0, false)},
    {1029, FileId(1029, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 143360, 0, false)},
    {2001, FileId(2001, 0, 0, 0, 0, execplan::CalpontSystemCatalog::VARCHAR, 8192, 0, true)},
    {2004, FileId(2004, 0, 0, 0, 4, execplan::CalpontSystemCatalog::INT, 24576, 0, true)},
};

void EMReBuilder::collectFileNames(const std::string& partialPath, std::string currentPath,
                                   std::vector<std::string>& fileNames)
{
  currentPath.append(partialPath);

  std::list<std::string> partialPathes;
  IDBPolicy::listDirectory(currentPath.c_str(), partialPathes);
  if (partialPathes.size() == 0)
  {
    fileNames.push_back(currentPath);
    return;
  }

  currentPath.push_back('/');
  for (const auto& partialPath : partialPathes)
    collectFileNames(partialPath, currentPath, fileNames);
}

int32_t EMReBuilder::collectExtents(const std::string& dbRootPath)
{
  if (doVerbose())
  {
    std::cout << "Collect extents for the DBRoot " << dbRootPath << std::endl;
  }

  std::vector<std::string> fileNames;
  collectFileNames(dbRootPath, "", fileNames);
  for (const auto& fileName : fileNames)
  {
    (void)collectExtent(fileName);
  }

  return 0;
}

int32_t EMReBuilder::collectExtent(const std::string& fullFileName)
{
  WriteEngine::FileOp fileOp;
  uint32_t oid;
  uint32_t partition;
  uint32_t segment;

  // Initialize oid, partition and segment from the given `fullFileName`.
  auto rc = WriteEngine::Convertor::fileName2Oid(fullFileName, oid, partition, segment);
  if (rc != 0)
    return rc;

  // Open the given file.
  std::unique_ptr<IDBDataFile> dbFile(IDBDataFile::open(IDBPolicy::getType(fullFileName, IDBPolicy::WRITEENG),
                                                        fullFileName.c_str(), "rb", 1));

  if (!dbFile)
  {
    std::cerr << "Cannot open file " << fullFileName << std::endl;
    return -1;
  }

  rc = dbFile->seek(0, 0);
  if (rc != 0)
  {
    std::cerr << "IDBDataFile::seek failed for the file " << fullFileName << std::endl;
    return rc;
  }

  if (oid > 3000)
  {
    // Read and verify header.
    char fileHeader[compress::CompressInterface::HDR_BUF_LEN * 2];
    rc = fileOp.readHeaders(dbFile.get(), fileHeader);
    if (rc != 0)
    {
      if (doVerbose())
      {
        std::cerr << "Cannot read file header from the file " << fullFileName
                  << ", probably this file was created without compression. " << std::endl;
      }
      return rc;
    }

    if (doVerbose())
    {
      std::cout << "Processing file: " << fullFileName << std::endl;
      std::cout << "fileName2Oid:  [OID: " << oid << ", partition: " << partition << ", segment: " << segment
                << "] " << std::endl;
    }

    // Read the `colDataType` and `colWidth` from the given header.
    const auto versionNumber = compress::CompressInterface::getVersionNumber(fileHeader);
    // Verify header number.
    if (versionNumber < 3)
    {
      if (doVerbose())
      {
        std::cerr << "File version " << versionNumber << " is not supported. " << std::endl;
      }
      return -1;
    }

    auto colDataType = compress::CompressInterface::getColDataType(fileHeader);
    auto colWidth = compress::CompressInterface::getColumnWidth(fileHeader);
    auto blockCount = compress::CompressInterface::getBlockCount(fileHeader);
    auto lbidCount = compress::CompressInterface::getLBIDCount(fileHeader);
    auto compressionType = compress::CompressInterface::getCompressionType(fileHeader);

    if (colDataType == execplan::CalpontSystemCatalog::UNDEFINED)
    {
      if (doVerbose())
        std::cout << "File header has invalid data. " << std::endl;

      return -1;
    }

    auto isDict = isDictFile(colDataType, colWidth);
    if (isDict)
      colWidth = 8;

    if (doVerbose())
    {
      std::cout << "Searching for HWM... " << std::endl;
      std::cout << "Block count: " << blockCount << std::endl;
    }

    uint64_t hwm = 0;
    rc = searchHWMInSegmentFile(fullFileName, oid, getDBRoot(), partition, segment, colDataType, colWidth,
                                blockCount, isDict, compressionType, hwm);

    if (rc != 0)
      return rc;

    if (doVerbose())
      std::cout << "HWM is: " << hwm << std::endl;

    const uint32_t extentMaxBlockCount = getEM().getExtentRows() * colWidth / BLOCK_SIZE;
    // We found multiple extents per one segment file.
    if (hwm >= extentMaxBlockCount)
    {
      for (uint32_t lbidIndex = 0; lbidIndex < lbidCount - 1; ++lbidIndex)
      {
        auto lbid = compress::CompressInterface::getLBIDByIndex(fileHeader, lbidIndex);
        FileId fileId(oid, partition, segment, getDBRoot(), colWidth, colDataType, lbid, /*hwm*/ 0, isDict);
        extentMap.push_back(fileId);
      }

      // Last one has an actual HWM.
      auto lbid = compress::CompressInterface::getLBIDByIndex(fileHeader, lbidCount - 1);
      FileId fileId(oid, partition, segment, getDBRoot(), colWidth, colDataType, lbid, hwm, isDict);
      extentMap.push_back(fileId);

      if (doVerbose())
      {
        std::cout << "Found multiple extents per segment file " << std::endl;
        std::cout << "FileId is collected " << fileId << std::endl;
      }
    }
    else
    {
      // One extent per segment file.
      auto lbid = compress::CompressInterface::getLBIDByIndex(fileHeader, 0);
      FileId fileId(oid, partition, segment, getDBRoot(), colWidth, colDataType, lbid, hwm, isDict);
      extentMap.push_back(fileId);

      if (doVerbose())
        std::cout << "FileId is collected " << fileId << std::endl;
    }
  }
  else
  {
    const auto fileSize = dbFile->size();
    if (fileSize == -1)
    {
      std::cerr << "IDBDataFile::size failed for the file " << fullFileName << std::endl;
      return -1;
    }

    const uint64_t blockCount = fileSize / BLOCK_SIZE;
    if (doVerbose())
    {
      std::cout << "Searching for HWM for system catalog file..." << std::endl;
      std::cout << "Block count: " << blockCount << std::endl;
    }

    auto it = systemCatalogMap.find(oid);
    if (it == systemCatalogMap.end())
    {
      std::cout << "Cannot find a system extent for OID: " << oid << std::endl;
      std::cout << "Continuing..." << std::endl;
      return 0;
    }

    FileId systemFileId = it->second;
    uint64_t hwm = 0;
    rc = searchHWMInSegmentFile(fullFileName, oid, getDBRoot(), systemFileId.partition, systemFileId.segment,
                                systemFileId.colDataType, systemFileId.colWidth, blockCount,
                                systemFileId.isDict, 0 /*=compressionType*/, hwm);
    if (rc != 0)
      return rc;

    if (doVerbose())
      std::cout << "HWM is: " << hwm << std::endl;

    systemFileId.hwm = hwm;
    systemFileId.dbroot = getDBRoot();
    systemExtentMap.push_back(systemFileId);
  }

  return 0;
}

int32_t EMReBuilder::rebuildExtentMap()
{
  // Initialize `extentMap` with system extents.
  extentMap.insert(extentMap.end(), systemExtentMap.begin(), systemExtentMap.end());

  if (doVerbose())
    std::cout << "Build extent map with size " << extentMap.size() << std::endl;

  // We have to restore extent map by restoring individual extent in order
  // they were created. This is important part, otherwise we will get invalid
  // extents for dictionary segment files and will not be able to access
  // columns through dictionay segment files.
  sort(extentMap.begin(), extentMap.end(),
       [](const FileId& lhs, const FileId& rhs) { return lhs.lbid < rhs.lbid; });

  for (const auto& fileId : extentMap)
  {
    uint32_t startBlockOffset;
    int32_t allocdSize;
    BRM::LBID_t lbid;

    if (!doDisplay())
    {
      try
      {
        if (fileId.isDict)
        {
          // Create a dictionary extent for the given oid, partition,
          // segment, dbroot.
          getEM().createDictStoreExtent(fileId.oid, fileId.dbroot, fileId.partition, fileId.segment, lbid,
                                        allocdSize);
        }
        else
        {
          // Create a column extent for the given oid, partition,
          // segment, dbroot and column width.
          getEM().createColumnExtentExactFile(fileId.oid, fileId.colWidth, fileId.dbroot, fileId.partition,
                                              fileId.segment, fileId.colDataType, lbid, allocdSize,
                                              startBlockOffset);
        }
      }
      // Could throw an logic_error exception.
      catch (std::exception& e)
      {
        getEM().undoChanges();
        std::cerr << "Cannot create column extent: " << e.what() << std::endl;
        return -1;
      }

      getEM().confirmChanges();
      if (doVerbose())
      {
        std::cout << "Extent is created, allocated size " << allocdSize << " actual LBID " << lbid
                  << std::endl;
        std::cout << "For " << fileId << std::endl;
      }

      // This is important part, it sets a status for specific extent
      // as 'available' that means we can use it.
      if (doVerbose())
      {
        std::cout << "Setting a HWM for " << fileId << std::endl;
      }
      try
      {
        getEM().setLocalHWM(fileId.oid, fileId.partition, fileId.segment, fileId.hwm, false, true);
      }
      catch (std::exception& e)
      {
        getEM().undoChanges();
        std::cerr << "Cannot set local HWM: " << e.what() << std::endl;
        return -1;
      }
      getEM().confirmChanges();
    }
  }
  return 0;
}

int32_t EMReBuilder::searchHWMInSegmentFile(const std::string& fullFileName, uint32_t oid, uint32_t dbRoot,
                                            uint32_t partition, uint32_t segment,
                                            execplan::CalpontSystemCatalog::ColDataType colDataType,
                                            uint32_t colWidth, uint64_t blockCount, bool isDict,
                                            uint32_t compressionType, uint64_t& hwm)
{
  std::unique_ptr<ChunkManagerWrapper> chunkManagerWrapper;
  try
  {
    if (isDict)
    {
      chunkManagerWrapper = std::unique_ptr<ChunkManagerWrapperDict>(new ChunkManagerWrapperDict(
          fullFileName, oid, dbRoot, partition, segment, colDataType, colWidth, compressionType));
    }
    else
    {
      chunkManagerWrapper = std::unique_ptr<ChunkManagerWrapperColumn>(new ChunkManagerWrapperColumn(
          fullFileName, oid, dbRoot, partition, segment, colDataType, colWidth, compressionType));
    }
  }
  catch (...)
  {
    std::cerr << "Cannot create ChunkManagerWrapper" << std::endl;
    return -1;
  }

  hwm = 0;
  // Starting from the end.
  // Note: This solves problem related to `bulk` insertion.
  // Currently it could start to insert values from any block into empty
  // column.
  for (int32_t currentBlock = blockCount - 1; currentBlock >= 0; --currentBlock)
  {
    // Read the block associated to HWM.
    // The uncompressed chunk size is 512 * 1024 * 8, so for `abbreviated`
    // extent one chunk will hold all blocks, therefore we need to
    // decompress chunk only once, the cached chunk will be used later.
    chunkManagerWrapper->readBlock(currentBlock);
    if (!chunkManagerWrapper->isEmptyBlock())
    {
      hwm = currentBlock;
      break;
    }
  }

  return 0;
}

void EMReBuilder::showExtentMap()
{
  std::cout << "range.start|range.size|fileId|blockOffset|HWM|partition|"
               "segment|dbroot|width|status|hiVal|loVal|seqNum|isValid|"
            << std::endl;
  getEM().dumpTo(std::cout);
}

// Check for hard-coded values which define dictionary file.
bool EMReBuilder::isDictFile(execplan::CalpontSystemCatalog::ColDataType colDataType, uint64_t width)
{
  // Dictionary store extent has a width == 0. See more detais in
  // `createDictStoreExtent` function.
  return (colDataType == execplan::CalpontSystemCatalog::VARCHAR) && (width == 0);
}

ChunkManagerWrapper::ChunkManagerWrapper(const std::string& filename, uint32_t oid, uint32_t dbRoot,
                                         uint32_t partition, uint32_t segment,
                                         execplan::CalpontSystemCatalog::ColDataType colDataType,
                                         uint32_t colWidth, uint32_t compressionType)
 : oid(oid)
 , dbRoot(dbRoot)
 , partition(partition)
 , segment(segment)
 , colDataType(colDataType)
 , colWidth(colWidth)
 , compressionType(compressionType)
 , size(colWidth)
 , pFileOp(nullptr)
 , fileName(filename)
{
}

int32_t ChunkManagerWrapper::readBlock(uint32_t blockNumber)
{
  int32_t rc = 0;

  if (compressionType == 0)
    rc = pFileOp->readDbBlocks(pFile, blockData, blockNumber, 1);
  else
    rc = chunkManager.readBlock(pFile, blockData, blockNumber);

  return rc;
}

ChunkManagerWrapperColumn::ChunkManagerWrapperColumn(const std::string& filename, uint32_t oid,
                                                     uint32_t dbRoot, uint32_t partition, uint32_t segment,
                                                     execplan::CalpontSystemCatalog::ColDataType colDataType,
                                                     uint32_t colWidth, uint32_t compressionType)
 : ChunkManagerWrapper(filename, oid, dbRoot, partition, segment, colDataType, colWidth, compressionType)
{
  if (compressionType == 0)
  {
    pFileOp = std::unique_ptr<WriteEngine::DbFileOp>(new WriteEngine::DbFileOp());
    pFile = IDBDataFile::open(IDBPolicy::getType(filename, IDBPolicy::WRITEENG), filename.c_str(), "rb",
                              colWidth);
  }
  else
  {
    pFileOp =
        std::unique_ptr<WriteEngine::ColumnOpCompress1>(new WriteEngine::ColumnOpCompress1(compressionType));
    chunkManager.fileOp(pFileOp.get());
    // Open compressed column segment file. We will read block by block
    // from the compressed chunks.
    pFile = chunkManager.getFilePtrByName(fileName, oid, dbRoot, partition, segment, colDataType, colWidth,
                                          "rb", size, false, false);
  }

  if (!pFile)
    throw std::bad_alloc();

  emptyValue = pFileOp->getEmptyRowValue(colDataType, colWidth);
  midOffset = (WriteEngine::BYTE_PER_BLOCK / 2);
  endOffset = (WriteEngine::BYTE_PER_BLOCK - colWidth);
}

bool ChunkManagerWrapperColumn::isEmptyBlock()
{
  const uint8_t* beginValue = blockData;
  const uint8_t* midValue = blockData + midOffset;
  const uint8_t* endValue = blockData + endOffset;

  return isEmptyValue(beginValue) && isEmptyValue(midValue) && isEmptyValue(endValue);
}

// This function is copy pasted from `ColumnOp` file, unfortunately it's not
// possible to reuse it directly form `ColumnOp` without a creating a some kind
// of object which inherits from `ColumnOp`, probably this should be moved to
// utils and make a static as well, to be able to use it without creating an
// object.
bool ChunkManagerWrapperColumn::isEmptyValue(const uint8_t* value) const
{
  switch (colWidth)
  {
    case 1: return *(uint8_t*)value == *(uint8_t*)emptyValue;

    case 2: return *(uint16_t*)value == *(uint16_t*)emptyValue;

    case 4: return *(uint32_t*)value == *(uint32_t*)emptyValue;

    case 8: return *(uint64_t*)value == *(uint64_t*)emptyValue;

    case 16: return *(uint128_t*)value == *(uint128_t*)emptyValue;
  }

  return false;
}

ChunkManagerWrapperDict::ChunkManagerWrapperDict(const std::string& filename, uint32_t oid, uint32_t dbRoot,
                                                 uint32_t partition, uint32_t segment,
                                                 execplan::CalpontSystemCatalog::ColDataType colDataType,
                                                 uint32_t colWidth, uint32_t compressionType)
 : ChunkManagerWrapper(filename, oid, dbRoot, partition, segment, colDataType, colWidth, compressionType)
{

  if (compressionType == 0)
  {
    pFileOp = std::unique_ptr<WriteEngine::DbFileOp>(new WriteEngine::DbFileOp());
    pFile = IDBDataFile::open(IDBPolicy::getType(filename, IDBPolicy::WRITEENG), filename.c_str(), "rb",
                              colWidth);
  }
  else
  {
    pFileOp =
        std::unique_ptr<WriteEngine::DctnryCompress1>(new WriteEngine::DctnryCompress1(compressionType));
    chunkManager.fileOp(pFileOp.get());
    // Open compressed dict segment file.
    pFile = chunkManager.getFilePtrByName(fileName, oid, dbRoot, partition, segment, colDataType, colWidth,
                                          "rb", size, false, true);
  }

  if (!pFile)
    throw std::bad_alloc();

  auto dictBlockHeaderSize = WriteEngine::HDR_UNIT_SIZE + WriteEngine::NEXT_PTR_BYTES +
                             WriteEngine::HDR_UNIT_SIZE + WriteEngine::HDR_UNIT_SIZE;

  emptyBlock = WriteEngine::BYTE_PER_BLOCK - dictBlockHeaderSize;
}

bool ChunkManagerWrapperDict::isEmptyBlock()
{
  // Check that block is completely empty.
  return (*(uint16_t*)blockData) == emptyBlock;
}

std::ostream& operator<<(std::ostream& os, const FileId& fileID)
{
  os << "[OID: " << fileID.oid << ", partition: " << fileID.partition << ", segment: " << fileID.segment
     << ", col width: " << fileID.colWidth << ", lbid:" << fileID.lbid << ", hwm: " << fileID.hwm
     << ", isDict: " << fileID.isDict << "]";
  return os;
}
}  // namespace RebuildExtentMap
