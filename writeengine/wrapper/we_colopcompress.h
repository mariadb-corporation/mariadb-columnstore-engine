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

//  $Id: we_colopcompress.h 4726 2013-08-07 03:38:36Z bwilkinson $

/** @file */

#pragma once

#include <cstdlib>

#include "we_colop.h"
#include "we_chunkmanager.h"
#include "calpontsystemcatalog.h"

#define EXPORT

/** Namespace WriteEngine */
namespace WriteEngine
{
/** Class ColumnOpCompress0 */
class ColumnOpCompress0 : public ColumnOp
{
 public:
  /**
   * @brief Constructor
   */
  EXPORT ColumnOpCompress0();
  EXPORT ColumnOpCompress0(Log* logger);

  /**
   * @brief Default Destructor
   */
  EXPORT ~ColumnOpCompress0() override;

  /**
   * @brief virtual method in ColumnOp
   */
  IDBDataFile* openFile(const Column& column, uint16_t dbRoot, uint32_t partition, uint16_t segment,
                        std::string& segFile, bool useTmpSuffix, const char* mode = "r+b",
                        int ioBuffSize = DEFAULT_BUFSIZ, bool isReadOnly = false) const override;

  /**
   * @brief virtual method in ColumnOp
   */
  bool abbreviatedExtent(IDBDataFile* pFile, int colWidth) const override;

  /**
   * @brief virtual method in ColumnOp
   */
  int blocksInFile(IDBDataFile* pFile) const override;

 protected:
  /**
   * @brief virtual method in ColumnOp
   */
  int readBlock(IDBDataFile* pFile, unsigned char* readBuf, const uint64_t fbo) override;

  /**
   * @brief virtual method in ColumnOp
   */
  int saveBlock(IDBDataFile* pFile, const unsigned char* writeBuf, const uint64_t fbo) override;

 private:
};

/** Class ColumnOpCompress1 */
class ColumnOpCompress1 : public ColumnOp
{
 public:
  /**
   * @brief Constructor
   */
  EXPORT ColumnOpCompress1(uint32_t compressionType, Log* logger = nullptr);

  /**
   * @brief Default Destructor
   */
  EXPORT ~ColumnOpCompress1() override;

  /**
   * @brief virtual method in FileOp
   */
  EXPORT int flushFile(int rc, std::map<FID, FID>& columnOids) override;

  /**
   * @brief virtual method in FileOp
   */
  int expandAbbrevColumnExtent(IDBDataFile* pFile, uint16_t dbRoot, const uint8_t* emptyVal, int width,
                               execplan::CalpontSystemCatalog::ColDataType colDataType) override;

  /**
   * @brief virtual method in ColumnOp
   */
  IDBDataFile* openFile(const Column& column, uint16_t dbRoot, uint32_t partition, uint16_t segment,
                        std::string& segFile, bool useTmpSuffix, const char* mode = "r+b",
                        int ioBuffSize = DEFAULT_BUFSIZ, bool isReadOnly = false) const override;

  /**
   * @brief virtual method in ColumnOp
   */
  bool abbreviatedExtent(IDBDataFile* pFile, int colWidth) const override;

  /**
   * @brief virtual method in ColumnOp
   */
  int blocksInFile(IDBDataFile* pFile) const override;

  //   void chunkManager(ChunkManager* cm);

  /**
   * @brief virtual method in FileOp
   */
  void setTransId(const TxnID& transId) override
  {
    ColumnOp::setTransId(transId);

    if (m_chunkManager)
      m_chunkManager->setTransId(transId);
  }

  void setBulkFlag(bool isBulkLoad) override
  {
    m_chunkManager->setBulkFlag(isBulkLoad);
  };

  void setFixFlag(bool isFix) override
  {
    m_chunkManager->setFixFlag(isFix);
  };

 protected:
  /**
   * @brief virtual method in FileOp
   */
  int updateColumnExtent(IDBDataFile* pFile, int nBlocks, int64_t lbid) override;

  /**
   * @brief virtual method in ColumnOp
   */
  void closeColumnFile(Column& column) const override;

  /**
   * @brief virtual method in ColumnOp
   */
  int readBlock(IDBDataFile* pFile, unsigned char* readBuf, const uint64_t fbo) override;

  /**
   * @brief virtual method in ColumnOp
   */
  int saveBlock(IDBDataFile* pFile, const unsigned char* writeBuf, const uint64_t fbo) override;

  /**
   * @brief Set the IsInsert flag in the ChunkManager.
   * This forces flush at end of statement. Used only for bulk insert.
   */
  void setIsInsert(bool isInsert)
  {
    m_chunkManager->setIsInsert(isInsert);
  }

 private:
};

}  // namespace WriteEngine

#undef EXPORT
