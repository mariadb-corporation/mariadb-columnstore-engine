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

//  $Id: we_colopcompress.cpp 4726 2013-08-07 03:38:36Z bwilkinson $


/** @file */

#include <stdio.h>
#include <string.h>
#include <vector>

#include "we_log.h"
#include "we_chunkmanager.h"
#include "idbcompress.h"

#include "we_colopcompress.h"

namespace WriteEngine
{

class ChunkManager;

// --------------------------------------------------------------------------------------------
// ColumnOp with compression type 0
// --------------------------------------------------------------------------------------------

/**
 * Constructor
 */
ColumnOpCompress0::ColumnOpCompress0()
{
   m_compressionType = 0;
}


ColumnOpCompress0::ColumnOpCompress0(Log* logger)
{
   m_compressionType = 0;
   setDebugLevel( logger->getDebugLevel() );
   setLogger    ( logger );
}

/**
 * Default Destructor
 */
ColumnOpCompress0::~ColumnOpCompress0()
{}


// @bug 5572 - HDFS usage: add *.tmp file backup flag
IDBDataFile* ColumnOpCompress0::openFile(
   const Column& column, const uint16_t dbRoot, const uint32_t partition, const uint16_t segment,
   std::string& segFile, bool useTmpSuffix, const char* mode, const int ioBuffSize) const
{
   return FileOp::openFile(column.dataFile.fid, dbRoot, partition, segment, segFile,
       mode, column.colWidth, useTmpSuffix);
}


bool ColumnOpCompress0::abbreviatedExtent(IDBDataFile* pFile, int colWidth) const
{
    long long fsize;
    if (getFileSize(pFile, fsize) == NO_ERROR)
    {
        return (fsize == INITIAL_EXTENT_ROWS_TO_DISK*colWidth);
    }
    // TODO: Log error
    return false;
}


int ColumnOpCompress0::blocksInFile(IDBDataFile* pFile) const
{
    long long fsize;
    if (getFileSize(pFile, fsize) == NO_ERROR)
    {
        return (fsize / BYTE_PER_BLOCK);
    }
    // TODO: Log error
    return 0;
}


int ColumnOpCompress0::readBlock(IDBDataFile* pFile, unsigned char* readBuf, const uint64_t fbo)
{
   return readDBFile(pFile, readBuf, fbo, true);
}


int ColumnOpCompress0::saveBlock(IDBDataFile* pFile, const unsigned char* writeBuf, const uint64_t fbo)
{
   return writeDBFileFbo(pFile, writeBuf, fbo, 1);
}


// --------------------------------------------------------------------------------------------
// ColumnOp with compression type 1
// --------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------

/**
 * Constructor
 */

ColumnOpCompress1::ColumnOpCompress1(Log* logger)
{
	m_compressionType = 1;
   m_chunkManager = new ChunkManager();
   if (logger)
   {
		setDebugLevel( logger->getDebugLevel() );
		setLogger    ( logger );
   }
   m_chunkManager->fileOp(this);
}

/**
 * Default Destructor
 */
ColumnOpCompress1::~ColumnOpCompress1()
{
	if (m_chunkManager)
	{
		delete m_chunkManager;
	}
}

// @bug 5572 - HDFS usage: add *.tmp file backup flag
IDBDataFile* ColumnOpCompress1::openFile(
   const Column& column, const uint16_t dbRoot, const uint32_t partition, const uint16_t segment,
   std::string& segFile, bool useTmpSuffix, const char* mode, const int ioBuffSize) const
{
    return m_chunkManager->getFilePtr(column, dbRoot, partition, segment, segFile,
        mode, ioBuffSize, useTmpSuffix);
}


bool ColumnOpCompress1::abbreviatedExtent(IDBDataFile* pFile, int colWidth) const
{
   return (blocksInFile(pFile) == INITIAL_EXTENT_ROWS_TO_DISK*colWidth/BYTE_PER_BLOCK);
}


int ColumnOpCompress1::blocksInFile(IDBDataFile* pFile) const
{
   CompFileHeader compFileHeader;
   readHeaders(pFile, compFileHeader.fControlData, compFileHeader.fPtrSection);

   compress::IDBCompressInterface compressor;
   return compressor.getBlockCount(compFileHeader.fControlData);
}


int ColumnOpCompress1::readBlock(IDBDataFile* pFile, unsigned char* readBuf, const uint64_t fbo)
{
   return m_chunkManager->readBlock(pFile, readBuf, fbo);
}


int ColumnOpCompress1::saveBlock(IDBDataFile* pFile, const unsigned char* writeBuf, const uint64_t fbo)
{
   return m_chunkManager->saveBlock(pFile, writeBuf, fbo);
}


int ColumnOpCompress1::flushFile(int rc, std::map<FID,FID> & columnOids)
{
   return m_chunkManager->flushChunks(rc, columnOids);
}


int ColumnOpCompress1::expandAbbrevColumnExtent(
   IDBDataFile* pFile, uint16_t dbRoot, uint64_t emptyVal, int width)
{
   // update the uncompressed initial chunk to full chunk
   int rc = m_chunkManager->expandAbbrevColumnExtent(pFile, emptyVal, width);
   // ERR_COMP_FILE_NOT_FOUND is acceptable here. It just means that the
   // file hasn't been loaded into the chunk manager yet. No big deal.
   if (rc != NO_ERROR && rc != ERR_COMP_FILE_NOT_FOUND)
   {
	   return rc;
   }

   // let the base to physically expand extent.
   return FileOp::expandAbbrevColumnExtent(pFile, dbRoot, emptyVal, width);
}


int ColumnOpCompress1::updateColumnExtent(IDBDataFile* pFile, int nBlocks)
{
   return m_chunkManager->updateColumnExtent(pFile, nBlocks);
}


void ColumnOpCompress1::closeColumnFile(Column& column) const
{
   // Leave file closing to chunk manager.
   column.dataFile.pFile = NULL;
}

} //end of namespace

