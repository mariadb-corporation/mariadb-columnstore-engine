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

#pragma once

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
#include "we_dbfileop.h"
#include <we_typeext.h>

#include "picosat.h"

using namespace idbdatafile;

namespace RebuildExtentMap
{
// This struct represents a FileId. For internal purpose only.
struct FileId
{
  FileId(uint32_t oid, uint32_t partition, uint32_t segment, uint32_t dbroot, uint32_t colWidth,
         execplan::CalpontSystemCatalog::ColDataType colDataType, int64_t lbid, uint64_t hwm, bool isDict,
         uint64_t blockOffset)
   : oid(oid)
   , partition(partition)
   , segment(segment)
   , dbroot(dbroot)
   , colWidth(colWidth)
   , colDataType(colDataType)
   , lbid(lbid)
   , hwm(hwm)
   , isDict(isDict)
   , blockOffset(blockOffset)
  {
  }

  FileId(const FileId& other)
   : oid(other.oid)
   , partition(other.partition)
   , segment(other.segment)
   , dbroot(other.dbroot)
   , colWidth(other.colWidth)
   , colDataType(other.colDataType)
   , lbid(other.lbid)
   , hwm(other.hwm)
   , isDict(other.isDict)
   , blockOffset(other.blockOffset)
  {
  }

  FileId()
   : colWidth(-1)
   , colDataType(execplan::CalpontSystemCatalog::INT)
  {
  }

  uint32_t oid;
  uint32_t partition;
  uint32_t segment;
  uint32_t dbroot;
  uint32_t colWidth;
  execplan::CalpontSystemCatalog::ColDataType colDataType;
  int64_t lbid;
  uint64_t hwm;
  bool isDict;
  uint64_t blockOffset;
};
std::ostream& operator<<(std::ostream& os, const FileId& fileID);

// This class represents extent map rebuilder.
class EMReBuilder
{
 public:
  EMReBuilder(bool verbose, bool display) : verbose(verbose), display(display)
  {
    // Initalize plugins.
    IDBPolicy::configIDBPolicy();
  }
  ~EMReBuilder() = default;

  // Collects extents from the given DBRoot path.
  int32_t collectExtents(const std::string& dbRootPath);

  // Collects file names for the given `partialPath` direcotory.
  void collectFileNames(const std::string& partialPath, std::string currentPath,
                        std::vector<std::string>& fileNames);

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

  // Solve all SAT problems for dictionaries.
  void solveExtents();

  // Rebuilds extent map from the collected map.
  int32_t rebuildExtentMap();

  // Search HWM in the given segment file.
  int32_t searchHWMInSegmentFile(const std::string& fullFileName, uint32_t oid, uint32_t dbRoot,
                                 uint32_t partition, uint32_t segment,
                                 execplan::CalpontSystemCatalog::ColDataType colDataType, uint32_t width,
                                 uint64_t blocksCount, bool isDict, bool probablyTokenColumn,
                                 uint32_t compressionType, uint64_t& hwm);

  // Sets the dbroot to the given `number`.
  void setDBRoot(uint32_t number)
  {
    dbRoot = number;
  }

  // Shows the extent map.
  void showExtentMap();

 private:
  struct SATProblem
  {
    int varIndex = 0;

    // template FileId to construct other FileIds.
    FileId templateFileId;

    // standard domain mapping between ranges and logical variables.
    std::map<uint64_t, int> fromRangeToVar;
    std::map<int, uint64_t> fromVarToRange;

    // to do not repeat ourselves, record starts of one-of encodings.
    std::set<uint64_t> oneOfStarts;

    // the problem itself, standard DIMACS body encoding: clauses end with 0,
    // positive integer is a variable, negative integer is a variable's negation.
    std::vector<int> problem;

    // LBIDs that are in header are marked as already added to the list.
    std::set<uint64_t> headerLBIDs;

    // invent new var or return existing for a range identified by start block offset.
    int getRangeVar(uint64_t range)
    {
      if (fromRangeToVar.count(range) > 0)
      {
        return fromRangeToVar[range];
      }
      varIndex ++; // preincrement because vars should not be
      int v = varIndex;
      fromRangeToVar[range] = v;
      fromVarToRange[v] = range;
      return v;
    }

    // 1-from-N encoding.
    void oneOf(const std::vector<int>& vars)
    {
      // encode requirement that at least one of vars is set.
      for(uint32_t i = 0; i < vars.size();i++) {
        problem.push_back(vars[i]);
      }
      problem.push_back(0); // close clause

      // encode requirement that if any variable is set to true, other
      // must be set to false.
      // This is quadratic to the number of vars, but simple and results
      // in faster solution process.
      for(uint32_t i = 0; i < vars.size(); i++) {
        int a = vars[i];
        for(uint32_t j = i + 1; j < vars.size(); j++) {
          int b = vars[j];
	  problem.push_back(-a);
	  problem.push_back(-b);
	  problem.push_back(0);
        }
      }
    }

    // 1-from-N encoding for ranges' range.
    // Do not do anything if we already added such subproblem.
    void oneOf(uint64_t start, uint64_t step, int n)
    {
      if (oneOfStarts.count(start) > 0)
      {
        return ; // do not repeat ourselves.
      }
      std::vector<int> vars;
      for(int i = 0;i < n; i++, start += step)
      {
        int var = getRangeVar(start);
	vars.push_back(var);
      }
      oneOf(vars);
    }

    void mustBe(uint64_t range)
    {
      int v = getRangeVar(range);
      problem.push_back(v); // assert presence.
      problem.push_back(0);
    }

    void headerLBID(uint64_t range)
    {
      if (headerLBIDs.count(range))
      {
        return ;
      }
      headerLBIDs.insert(range);
      mustBe(range);
    }

    void mustNotBe(uint64_t range)
    {
      int v = getRangeVar(range);
      problem.push_back(-v); // assert absence.
      problem.push_back(0);
    }

    // add the fact that some FBO is utilized - it may require one of ranges and
    // also prevent use of some ranges.
    void addWrittenFBO(uint64_t fbo, uint64_t lengthInBlocks)
    {
      // why 512 - this is least possible extent size in blocks.
      const uint64_t smallestAlignment = 512;
      const uint64_t extentSize = 8192;
      idbassert(fbo < std::numeric_limits<uint64_t>::max() - extentSize);

      uint64_t highest_offset = fbo - (fbo % smallestAlignment);
      uint64_t lowest_offset = 0;
      if (highest_offset >= extentSize)
      {
        lowest_offset = highest_offset + smallestAlignment - extentSize;
      }
      int n = (highest_offset - lowest_offset) / smallestAlignment;
      oneOf(highest_offset, smallestAlignment, n);
      uint64_t dataEnd = fbo + lengthInBlocks;
      if (dataEnd / smallestAlignment != fbo / smallestAlignment) // may span adjacent ranges.
      {
        // mark as invalid ranges whose bounds are inside written data.
	for(uint64_t end = highest_offset + smallestAlignment; end < dataEnd; end += smallestAlignment)
	{
	  if (end >= extentSize)
	  {
	    uint64_t start = end - extentSize;
	    mustNotBe(start);
	  }
	  if (end < std::numeric_limits<uint64_t>::max() - extentSize)
	  {
	    mustNotBe(end);
	  }
	}
      }
    }

    void setTemplateFileId(const FileId& fileId)
    {
      templateFileId = fileId;
    }

    void solve(std::vector<FileId>& ranges)
    {
      PicoSAT* psat = picosat_init();
      idbassert(psat);
      for(int lit : problem)
      {
        picosat_add(psat, lit);
      }
      int retCode = picosat_sat(psat, -1);

      idbassert(retCode == PICOSAT_SATISFIABLE); // we always generate satisfiable problems.

      // obtaining the values of literals.
      // We are interested only in 1 values (assigned to true).
      for(int v = 1; v < varIndex; v++)
      {
        int l = picosat_deref(psat, v);
	if (l > 0)
	{
	  // this var is assigned true, meaning it's range allowed into distribution..
	  uint64_t range = fromVarToRange[v];
	  if (headerLBIDs.count(range) < 1)
	  {
	    // not a header range so we need to generate a FileId.
	    FileId fid(templateFileId);
	    fid.lbid = range;
            ranges.push_back(fid);
	  }
	}
      }
      picosat_reset(psat);
    }

  };

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
  std::vector<FileId> systemExtentMap;
  std::vector<FileId> extentMap;

  std::map<uint32_t, uint64_t> oidHWMs; // HWM is assigned at the very end, to the LBID that properly contains it.
  uint64_t lastUsedLBID = 0;
  std::set<uint32_t> dictOIDs; // set of OIDs that are dicts.
  std::map<uint32_t, SATProblem> problems;
  std::map<uint32_t, std::set<uint64_t>> oidBlockOffsetsFromTokens; // block offsets generated from tokens read.
  std::map<uint32_t, std::set<uint64_t>> oidKnownBlockOffsets; // the set of block offsets that need not new LBIDs.

  // TEXT and other long variable length columns are stored as two OIDs, one (OID) for tokens and other
  // (OID+1) for the actual data.
  // This method receives OID+1 - we scan tokens for LBIDs that belong to the the next OID file(s).
  // All this method do is to fill SATProblem for OID+1 to solve later.
  void scanTokensForLBIDs(uint32_t oid, const WriteEngine::Token* tokens, uint32_t numTokens, std::set<uint64_t>& seen);

  // generate FileId's for invisible LBIDs - these are not recorded in headers.
  void addInvisibleLBIDs();

  // setup HWMs for all OIDs.
  void setupHWMs();


};

// The base class aroud `ChunkManager` to read and write decompressed blocks
// from segment file.
class ChunkManagerWrapper
{
 public:
  ChunkManagerWrapper(const std::string& filename, uint32_t oid, uint32_t dbRoot, uint32_t partition,
                      uint32_t segment, execplan::CalpontSystemCatalog::ColDataType colDataType,
                      uint32_t colWidth, uint32_t compressionType);

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

  // return internal buffer as an array of tokens
  const WriteEngine::Token* getTokens() const { return reinterpret_cast<const WriteEngine::Token*>(blockData); }

  // convenience: return nummber of tokens in block.
  uint32_t numTokens() const { return WriteEngine::BYTE_PER_BLOCK/sizeof(WriteEngine::Token); }

 protected:
  uint32_t oid;
  uint32_t dbRoot;
  uint32_t partition;
  uint32_t segment;
  execplan::CalpontSystemCatalog::ColDataType colDataType;
  uint32_t colWidth;
  uint32_t compressionType;
  int32_t size;
  std::unique_ptr<WriteEngine::DbFileOp> pFileOp;
  std::string fileName;
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
  ChunkManagerWrapperColumn(const std::string& filename, uint32_t oid, uint32_t dbRoot, uint32_t partition,
                            uint32_t segment, execplan::CalpontSystemCatalog::ColDataType colDataType,
                            uint32_t colWidth, uint32_t compressionType);

  ~ChunkManagerWrapperColumn()
  {
    // In case we open file without `ChunkManager` machinery.
    if (!compressionType && pFile)
      delete pFile;
  };

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
  ChunkManagerWrapperDict(const std::string& filename, uint32_t oid, uint32_t dbRoot, uint32_t partition,
                          uint32_t segment, execplan::CalpontSystemCatalog::ColDataType colDataType,
                          uint32_t colWidth, uint32_t compressionType);

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
