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

#ifndef REBUILD_EM_H
#define REBUILD_EM_H

#include <string>
#include <map>
#include <ftw.h>

#include "calpontsystemcatalog.h"
#include "extentmap.h"
#include "IDBPolicy.h"
#include "IDBFileSystem.h"
#include "idbcompress.h"
#include "blocksize.h"
#include "we_convertor.h"
#include "we_fileop.h"
#include "IDBPolicy.h"
#include "we_chunkmanager.h"

using namespace idbdatafile;

namespace RebuildExtentMap
{
// This struct represents a FileId. For internal purpose only.
struct FileId
{
  FileId(uint32_t oid, uint32_t partition, uint32_t segment, uint32_t colWidth,
         execplan::CalpontSystemCatalog::ColDataType colDataType, int64_t lbid, uint64_t hwm, bool isDict)
   : oid(oid)
   , partition(partition)
   , segment(segment)
   , colWidth(colWidth)
   , colDataType(colDataType)
   , lbid(lbid)
   , hwm(hwm)
   , isDict(isDict)
  {
  }

  uint32_t oid;
  uint32_t partition;
  uint32_t segment;
  uint32_t colWidth;
  execplan::CalpontSystemCatalog::ColDataType colDataType;
  int64_t lbid;
  uint64_t hwm;
  bool isDict;
};
std::ostream& operator<<(std::ostream& os, const FileId& fileID);

// This class represents extent map rebuilder.
class EMReBuilder
{
 public:
  EMReBuilder(bool verbose, bool display) : verbose(verbose), display(display)
  {
    // Initalize plugins.
    IDBPolicy::init(true, false, "", 0);
  }
  ~EMReBuilder() = default;

  // Collects extents from the given DBRoot path.
  int32_t collectExtents(const std::string& dbRootPath);

  // Clears collected extents.
  void clear()
  {
    extentMap.clear();
  }

  // Specifies whether we need verbose to output.
  bool doVerbose() const
  {
    return verbose;
  }

  // Specifies whether we need just display a pipeline, but not actually run
  // it.
  bool doDisplay() const
  {
    return display;
  }

  // Returns the number of current DBRoot.
  uint32_t getDBRoot() const
  {
    return dbRoot;
  }

  // Retunrs a reference to `ExtentMap` object.
  BRM::ExtentMap& getEM()
  {
    return em;
  }

  // Checks if the given data specifies a dictionary file.
  static bool isDictFile(execplan::CalpontSystemCatalog::ColDataType colDataType, uint64_t width);

  // Initializes system extents from the binary blob.
  // This function solves the problem related to system segment files.
  // Currently those files do not have file header, so we cannot
  // get the data (like width, colType, lbid) to restore an extent for this
  // particular segment file. The current approach is to keep a binary blob
  // of initial state of the system extents.
  // Returns -1 on error.
  int32_t initializeSystemExtents();

  // Rebuilds extent map from the collected map.
  int32_t rebuildExtentMap();

  // Search HWM in the given segment file.
  int32_t searchHWMInSegmentFile(uint32_t oid, uint32_t dbRoot, uint32_t partition, uint32_t segment,
                                 execplan::CalpontSystemCatalog::ColDataType colDataType, uint32_t width,
                                 uint64_t blocksCount, bool isDict, uint32_t compressionType, uint64_t& hwm);

  // Sets the dbroot to the given `number`.
  void setDBRoot(uint32_t number)
  {
    dbRoot = number;
  }

  // Shows the extent map.
  void showExtentMap();

 private:
  EMReBuilder(const EMReBuilder&) = delete;
  EMReBuilder(EMReBuilder&&) = delete;
  EMReBuilder& operator=(const EMReBuilder&) = delete;
  EMReBuilder& operator=(EMReBuilder&&) = delete;

  // Collects the information for extent from the given file and stores
  // it in `extentMap` set.
  int32_t collectExtent(const std::string& fullFileName);
  bool verbose;
  bool display;
  uint32_t dbRoot;
  BRM::ExtentMap em;
  std::vector<FileId> extentMap;
};

// The base class aroud `ChunkManager` to read and write decompressed blocks
// from segment file.
class ChunkManagerWrapper
{
 public:
  ChunkManagerWrapper(uint32_t oid, uint32_t dbRoot, uint32_t partition, uint32_t segment,
                      execplan::CalpontSystemCatalog::ColDataType colDataType, uint32_t colWidth);

  virtual ~ChunkManagerWrapper() = default;
  ChunkManagerWrapper(const ChunkManagerWrapper& other) = delete;
  ChunkManagerWrapper& operator=(const ChunkManagerWrapper& other) = delete;
  ChunkManagerWrapper(ChunkManagerWrapper&& other) = delete;
  ChunkManagerWrapper& operator=(ChunkManagerWrapper&& other) = delete;

  // Reads block, by given `blockNumber` from associated segment file and
  // populates internal block buffer.
  int32_t readBlock(uint32_t blockNumber);

  // Checks that last read block is empty.
  virtual bool isEmptyBlock() = 0;

 protected:
  uint32_t oid;
  uint32_t dbRoot;
  uint32_t partition;
  uint32_t segment;
  execplan::CalpontSystemCatalog::ColDataType colDataType;
  uint32_t colWidth;
  int32_t size;
  std::string fileName;
  std::unique_ptr<WriteEngine::FileOp> pFileOp;
  // Note: We cannot clear this pointer directly, because
  // `ChunkManager` closes this file for us, otherwise we will get double
  // free error.
  IDBDataFile* pFile;
  WriteEngine::ChunkManager chunkManager;
  uint8_t blockData[WriteEngine::BYTE_PER_BLOCK];
};

// Class to read decompressed blocks from column segment files.
class ChunkManagerWrapperColumn : public ChunkManagerWrapper
{
 public:
  ChunkManagerWrapperColumn(uint32_t oid, uint32_t dbRoot, uint32_t partition, uint32_t segment,
                            execplan::CalpontSystemCatalog::ColDataType colDataType, uint32_t colWidth,
                            uint32_t compressionType);

  ~ChunkManagerWrapperColumn() = default;
  ChunkManagerWrapperColumn(const ChunkManagerWrapperColumn& other) = delete;
  ChunkManagerWrapperColumn& operator=(const ChunkManagerWrapperColumn& other) = delete;
  ChunkManagerWrapperColumn(ChunkManagerWrapperColumn&& other) = delete;
  ChunkManagerWrapperColumn& operator=(ChunkManagerWrapperColumn&& other) = delete;

  bool isEmptyBlock() override;
  bool isEmptyValue(const uint8_t* value) const;

 private:
  const uint8_t* emptyValue;
  uint32_t midOffset;
  uint32_t endOffset;
};

// Class to read decompressed blocks from dict segment files.
class ChunkManagerWrapperDict : public ChunkManagerWrapper
{
 public:
  ChunkManagerWrapperDict(uint32_t oid, uint32_t dbRoot, uint32_t partition, uint32_t segment,
                          execplan::CalpontSystemCatalog::ColDataType colDataType, uint32_t colWidth,
                          uint32_t compressionType);

  ~ChunkManagerWrapperDict() = default;
  ChunkManagerWrapperDict(const ChunkManagerWrapperDict& other) = delete;
  ChunkManagerWrapperDict& operator=(const ChunkManagerWrapperDict& other) = delete;
  ChunkManagerWrapperDict(ChunkManagerWrapperDict&& other) = delete;
  ChunkManagerWrapperDict& operator=(ChunkManagerWrapperDict&& other) = delete;

  bool isEmptyBlock() override;

 private:
  uint32_t emptyBlock;
};

}  // namespace RebuildExtentMap
#endif
