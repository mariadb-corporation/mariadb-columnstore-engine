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

// Define __STDC_LIMIT_MACROS so that we can pick up UINT64_MAX from stdint.h
#define __STDC_LIMIT_MACROS 1
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>
#include <stdint.h>
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include <cppunit/extensions/HelperMacros.h>

#include "we_dbfileop.h"
#include "we_type.h"
#include "we_log.h"
#include "we_convertor.h"
#include "we_brm.h"
#include "we_cache.h"
#include "we_colop.h"
#include "IDBDataFile.h"
#include "BufferedFile.h"
#include "IDBPolicy.h"
#include "IDBFileSystem.h"
#include "idbcompress.h"
#include "calpontsystemcatalog.h"
#include "we_colopcompress.h"
#include "we_dctnrycompress.h"

using namespace compress;
using namespace idbdatafile;
using namespace WriteEngine;
using namespace BRM;

/*int compare (const void* a, const void* b)
{
    return ( *(uint32_t*)a - * (uint32_t*)b );
}

int compare1(const void* a, const void* b)
{
    return ( (*(SortTuple*)a).key - (*(SortTuple*)b).key );
}
*/

class SharedTest : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE(SharedTest);

  CPPUNIT_TEST(setUp);
  // CPPUNIT_TEST( test1 );

  // File operation testing
  //    CPPUNIT_TEST( testFileNameOp );
  //    CPPUNIT_TEST( testFileHandleOp );
  CPPUNIT_TEST(testDirBasic);
  CPPUNIT_TEST(testCreateDeleteFile);

  // Data block related testing
  //    CPPUNIT_TEST( testCalculateRowIdBitmap );
  //    CPPUNIT_TEST( testBlockBuffer );
  //    CPPUNIT_TEST( testBitBasic );
  //    CPPUNIT_TEST( testBufferBit );
  //    CPPUNIT_TEST( testBitShift );
  //    CPPUNIT_TEST( testEmptyRowValue );
  //    CPPUNIT_TEST( testCorrectRowWidth );
  // DB File Block related testing
  //    CPPUNIT_TEST( testDbBlock );

  //    CPPUNIT_TEST( testCopyDbFile );

  // Extent & dict related testing
  CPPUNIT_TEST(testExtensionWOPrealloc);
  CPPUNIT_TEST(testDictExtensionWOPrealloc);
  //    CPPUNIT_TEST( testExtentCrWOPreallocBin );
  // Semaphore related testing
  //    CPPUNIT_TEST( testSem );

  // Log related testing
  CPPUNIT_TEST(testLog);

  // Version Buffer related testing
  //    CPPUNIT_TEST( testHWM );
  //    CPPUNIT_TEST( testVB );

  // Disk manager related testing
  //    CPPUNIT_TEST( testDM );
  //    CPPUNIT_TEST( tearDown );

  // Cache related testing
  //    CPPUNIT_TEST( testCacheBasic );
  //    CPPUNIT_TEST( testCacheReadWrite );

  CPPUNIT_TEST(testCleanup);  // NEVER COMMENT OUT THIS LINE
  CPPUNIT_TEST_SUITE_END();

 private:
 public:
  void setUp()
  {
  }

  void tearDown()
  {
  }

  void test1()
  {
    int numOfBlock = 1024;
    FILE* pFile;
    unsigned char writeBuf[BYTE_PER_BLOCK * 10];
    char diskBuf[BYTE_PER_BLOCK * 11];
    char fileName[40];

    for (int k = 0; k < 273; k++)
    {
      snprintf(fileName, sizeof(fileName), "testwr%d.tst", k);
      pFile = fopen(fileName, "w+b");

      if (pFile != NULL)
      {
        setvbuf(pFile, diskBuf, _IOFBF, 81920);
        memset(writeBuf, 0, BYTE_PER_BLOCK * sizeof(unsigned char));

        for (int i = 0; i < numOfBlock; i++)
          fwrite(writeBuf, sizeof(writeBuf), 1, pFile);

        //            fwrite( writeBuf, sizeof( writeBuf ), numOfBlock, pFile );
        fclose(pFile);
      }
    }
  }
  /*
      void testFileNameOp()
      {
          FileOp   fileOp;
          bool     bStatus;
          char     fileName[20];
          int      rc;

          strcpy( fileName, "FILE_999.dat" );
          BRMWrapper::setUseBrm(true);
          // file opertaions
          fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( fileOp.exists( fileName ) == false );

          rc = fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( rc == ERR_FILE_NOT_EXIST );

          rc = fileOp.createFile( fileName, 10 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = fileOp.createFile( fileName, 10 );
          CPPUNIT_ASSERT( rc == ERR_FILE_EXIST );

          bStatus = fileOp.exists( fileName );
          CPPUNIT_ASSERT( bStatus == true );

          // init different value
          fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( fileOp.exists( fileName ) == false );

          rc = fileOp.createFile( fileName, 10, 0xFF, 1 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( fileOp.exists( fileName ) == false );
          rc = fileOp.createFile( fileName, 10, 0x8001, 2 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( fileOp.exists( fileName ) == false );
          rc = fileOp.createFile( fileName, 10, 0xFFAAAAAB, 4 );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( fileOp.exists( fileName ) == false );

          //Not use block resolution manager
          BRMWrapper::setUseBrm(false);
          // file opertaions
          fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( fileOp.exists( fileName ) == false );

          rc = fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( rc == ERR_FILE_NOT_EXIST );

          rc = fileOp.createFile( fileName, 10 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = fileOp.createFile( fileName, 10 );
          CPPUNIT_ASSERT( rc == ERR_FILE_EXIST );

          bStatus = fileOp.exists( fileName );
          CPPUNIT_ASSERT( bStatus == true );

          // init different value
          fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( fileOp.exists( fileName ) == false );

          rc = fileOp.createFile( fileName, 10, 0xFF, 1 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( fileOp.exists( fileName ) == false );
          rc = fileOp.createFile( fileName, 10, 0x8001, 2 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( fileOp.exists( fileName ) == false );
          rc = fileOp.createFile( fileName, 10, 0xFFAAAAAB, 4 );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          fileOp.deleteFile( fileName );
          CPPUNIT_ASSERT( fileOp.exists( fileName ) == false );
      }

      void testFileHandleOp()
      {
          FileOp   fileOp;
          char     fileName[100], testStr[50];
          unsigned char dataBuf[BYTE_PER_BLOCK];
          long     fileSize;
          FILE*    pFile;
          int      rc;
          int      allocSize = 0;

          BRMWrapper::setUseBrm(true);
  //      strcpy( fileName, "FILE_999.dat" );
          strcpy( testStr, "String write test data" );

          fileOp.deleteFile( 999 );
          rc = fileOp.createFile( 999, 10, allocSize);
          CPPUNIT_ASSERT( rc == NO_ERROR );
          pFile = fileOp.openFile( 999 );
          CPPUNIT_ASSERT( pFile != NULL );

          fileSize = fileOp.getFileSize( 999 );
          CPPUNIT_ASSERT( fileSize == allocSize * BYTE_PER_BLOCK );

          rc = fileOp.setFileOffset( pFile, -1, SEEK_SET );
          CPPUNIT_ASSERT( rc == ERR_FILE_SEEK );

          uint64_t lbid1;
          rc = BRMWrapper::getInstance()->getBrmInfo( 1999, 1, lbid1 );
          CPPUNIT_ASSERT( rc != NO_ERROR );
          rc = BRMWrapper::getInstance()->getBrmInfo( 999, 1, lbid1 );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          rc = fileOp.setFileOffset( pFile, lbid1 * BYTE_PER_BLOCK, SEEK_SET );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          uint64_t lbid9;
          rc = BRMWrapper::getInstance()->getBrmInfo( 999, 9, lbid9 );

          CPPUNIT_ASSERT( rc == NO_ERROR );
          rc = fileOp.setFileOffset( pFile, BYTE_PER_BLOCK * lbid9, SEEK_SET );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          // write something to block 1
          memset( dataBuf, 0, sizeof( unsigned char ) * BYTE_PER_BLOCK );
          memcpy( dataBuf + 8, testStr, strlen( testStr ) );

          CPPUNIT_ASSERT( fileOp.setFileOffset( pFile, lbid1 * BYTE_PER_BLOCK, SEEK_SET ) == NO_ERROR );
          rc = fileOp.writeFile( pFile, dataBuf, BYTE_PER_BLOCK );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = fileOp.writeFile( NULL, dataBuf, BYTE_PER_BLOCK );
          CPPUNIT_ASSERT( rc == ERR_FILE_NULL );

          // read something from block 1
          memset( dataBuf, 0, sizeof( unsigned char ) * BYTE_PER_BLOCK );

          rc = BRMWrapper::getInstance()->getBrmInfo( 999, 1, lbid1 );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          CPPUNIT_ASSERT( fileOp.setFileOffset( pFile, lbid1 * BYTE_PER_BLOCK, SEEK_SET ) == NO_ERROR );
          rc = fileOp.readFile( pFile, dataBuf, BYTE_PER_BLOCK );

          printf( "\ndataBuf=%s\n", dataBuf + 8 );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          CPPUNIT_ASSERT( memcmp( dataBuf + 8, testStr, strlen(testStr)) == 0 );

          CPPUNIT_ASSERT( fileOp.getFileSize( 999 ) == allocSize * 8192 );
          fileOp.closeFile( pFile );
          fileOp.deleteFile(999);

          // No block resolution manager
          BRMWrapper::setUseBrm(false);
  //      strcpy( fileName, "FILE_999.dat" );
          rc = fileOp.getFileName( 999, fileName );
          strcpy( testStr, "String write test data" );
          fileOp.deleteFile( 999 );
          CPPUNIT_ASSERT( fileOp.exists( 999 ) == false );

          rc = fileOp.createFile( 999, 10, allocSize);
          CPPUNIT_ASSERT( rc == NO_ERROR );
          pFile = fileOp.openFile( fileName );
          CPPUNIT_ASSERT( pFile != NULL );

          fileSize = fileOp.getFileSize( 999 );
          CPPUNIT_ASSERT( fileSize == 81920 );

          rc = fileOp.setFileOffset( pFile, -1, SEEK_SET );
          CPPUNIT_ASSERT( rc == ERR_FILE_SEEK );

          rc = fileOp.setFileOffset( pFile, BYTE_PER_BLOCK, SEEK_SET );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = fileOp.setFileOffset( pFile, BYTE_PER_BLOCK * 9, SEEK_SET );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          // write something to block 1
          memset( dataBuf, 0, sizeof( unsigned char ) * BYTE_PER_BLOCK );
          memcpy( dataBuf + 8, testStr, strlen( testStr ) );

          CPPUNIT_ASSERT( fileOp.setFileOffset( pFile, BYTE_PER_BLOCK, SEEK_SET ) == NO_ERROR );
          rc = fileOp.writeFile( pFile, dataBuf, BYTE_PER_BLOCK );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = fileOp.writeFile( NULL, dataBuf, BYTE_PER_BLOCK );
          CPPUNIT_ASSERT( rc == ERR_FILE_NULL );

          // read something from block 1
          memset( dataBuf, 0, sizeof( unsigned char ) * BYTE_PER_BLOCK );

          CPPUNIT_ASSERT( fileOp.setFileOffset( pFile, BYTE_PER_BLOCK, SEEK_SET ) == NO_ERROR );
          rc = fileOp.readFile( pFile, dataBuf, BYTE_PER_BLOCK );

          printf( "\ndataBuf=%s\n", dataBuf + 8 );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          CPPUNIT_ASSERT( memcmp( dataBuf + 8, testStr, strlen(testStr)) == 0 );

          CPPUNIT_ASSERT( fileOp.getFileSize( 999 ) == 81920 );
          fileOp.closeFile( pFile );
          fileOp.deleteFile(999);

      }

  */
  void testDirBasic()
  {
    FileOp fileOp;
    char dirName[30];
    int rc;

    printf("\nRunning testDirBasic \n");
    idbdatafile::IDBPolicy::init(true, false, "", 0);
    IDBFileSystem& fs = IDBPolicy::getFs("/tmp");
    strcpy(dirName, "/tmp/testdir42");
    fs.remove(dirName);
    CPPUNIT_ASSERT(fileOp.isDir(dirName) == false);

    rc = fileOp.createDir(dirName);
    CPPUNIT_ASSERT(rc == NO_ERROR);
    CPPUNIT_ASSERT(fileOp.isDir(dirName) == true);

    fs.remove(dirName);
  }

  void testCreateDeleteFile()
  {
    IDBDataFile* pFile = NULL;
    FileOp fileOp;
    BlockOp blockOp;
    char fileName[20];
    int rc;
    char hdrs[CompressInterface::HDR_BUF_LEN * 2];

    printf("\nRunning testCreateDeleteFile \n");
    idbdatafile::IDBPolicy::init(true, false, "", 0);
    // Set to versionbuffer to satisfy IDBPolicy::getType
    strcpy(fileName, "versionbuffer");
    fileOp.compressionType(1);

    fileOp.deleteFile(fileName);
    CPPUNIT_ASSERT(fileOp.exists(fileName) == false);

    int width = blockOp.getCorrectRowWidth(execplan::CalpontSystemCatalog::BIGINT, 8);
    int nBlocks = INITIAL_EXTENT_ROWS_TO_DISK / BYTE_PER_BLOCK * width;
    const uint8_t* emptyVal = blockOp.getEmptyRowValue(execplan::CalpontSystemCatalog::BIGINT, 8);
    // createFile runs IDBDataFile::open + initAbrevCompColumnExtent
    // under the hood
    // bigint column file
    rc = fileOp.createFile(fileName,
                           nBlocks,   // number of blocks
                           emptyVal,  // NULL value
                           width,     // width
                           execplan::CalpontSystemCatalog::BIGINT,
                           1);  // dbroot
    CPPUNIT_ASSERT(rc == NO_ERROR);

    fileOp.closeFile(pFile);

    pFile = IDBDataFile::open(IDBPolicy::getType(fileName, IDBPolicy::WRITEENG), fileName, "rb", 1);

    rc = pFile->seek(0, 0);
    CPPUNIT_ASSERT(rc == NO_ERROR);
    rc = fileOp.readHeaders(pFile, hdrs);
    CPPUNIT_ASSERT(rc == NO_ERROR);
    // Couldn't use IDBDataFile->close() here w/o excplicit cast
    fileOp.closeFile(pFile);

    fileOp.deleteFile(fileName);
    CPPUNIT_ASSERT(fileOp.exists(fileName) == false);
  }
  /*
      void testCalculateRowIdBitmap()
      {
          BlockOp blockOp;
          RID rowId = numeric_limits<WriteEngine::RID>max();
          int  fbo = 0, bio = 0, bbo = 0;

          // Assuming 1024 per data block, 8 byte width
          CPPUNIT_ASSERT( blockOp.calculateRowId( rowId, 1024, 8, fbo, bio ) == false );

          rowId = 1026;
          CPPUNIT_ASSERT( blockOp.calculateRowId( rowId, 1024, 8, fbo, bio ) == true );
          CPPUNIT_ASSERT( fbo == 1 );
          CPPUNIT_ASSERT( bio == 16 );

          // Assuming 2048 per data block, 4 byte width
  //              rowId = 2049;
  //              CPPUNIT_ASSERT( blockOp.calculateRowId( rowId, 2048, 4, fbo, bio ) == true );
  //              CPPUNIT_ASSERT( fbo == 1 );
  //              CPPUNIT_ASSERT( bio == 16 );
  //
  //              // Assuming 4096 per data block, 2 byte width
  //              rowId = 2049;
  //              CPPUNIT_ASSERT( blockOp.calculateRowId( rowId, 4096, 2, fbo, bio ) == true );
  //              CPPUNIT_ASSERT( fbo == 1 );
  //              CPPUNIT_ASSERT( bio == 16 );
  //
  //              // Assuming 8192 per data block, 1 byte width
  //              rowId = 2049;
  //              CPPUNIT_ASSERT( blockOp.calculateRowId( rowId, 8192, 1, fbo, bio ) == true );
  //              CPPUNIT_ASSERT( fbo == 1 );
  //              CPPUNIT_ASSERT( bio == 16 );
  //
          rowId = 65546;
          CPPUNIT_ASSERT( blockOp.calculateRowBitmap( rowId, BYTE_PER_BLOCK * 8, fbo, bio, bbo ) == true );
          CPPUNIT_ASSERT( fbo == 1 );
          CPPUNIT_ASSERT( bio == 1 );
          CPPUNIT_ASSERT( bbo == 2 );

  //      CPPUNIT_ASSERT( blockOp.getRowId( fbo, bio, bbo ) == 65546 );

          rowId = numeric_limits<WriteEngine::RID>max();
          CPPUNIT_ASSERT( blockOp.calculateRowBitmap( rowId, BYTE_PER_BLOCK * 8, fbo, bio, bbo ) == false );
      }


      void testBlockBuffer()
      {
          BlockOp blockOp;
          unsigned char buf[BYTE_PER_BLOCK];
          int writeVal = 80, readVal;

          blockOp.resetBuf( buf, sizeof( buf ) );
          CPPUNIT_ASSERT( buf[1] == '\0' );

          // copy to byte 8 area
          blockOp.writeBufValue( buf + 8, &writeVal, 4, true );

  //      blockOp.writeBufValue( buf + 8, &writeVal, 4 );
          blockOp.readBufValue( buf + 8, &readVal, 4 );
          CPPUNIT_ASSERT( readVal == 80 );
      }

      void testBitBasic()
      {
          BlockOp blockOp;
          unsigned int val;

          val = 7;
          CPPUNIT_ASSERT( blockOp.getByteTotalOnBit( val, 8 ) == 3 );
          val = 33;
          CPPUNIT_ASSERT( blockOp.getByteTotalOnBit( val, 8 ) == 2 );
          val = 255;
          CPPUNIT_ASSERT( blockOp.getByteTotalOnBit( val, 8 ) == 8 );
          val = 0;
          CPPUNIT_ASSERT( blockOp.getByteTotalOnBit( val, 8 ) == 0 );

          val = 3;
          CPPUNIT_ASSERT( blockOp.getBitFlag( val, 0 ) == 1 );
          CPPUNIT_ASSERT( blockOp.getBitFlag( val, 1 ) == 1 );
          CPPUNIT_ASSERT( blockOp.getBitFlag( val, 2 ) == 0 );
          CPPUNIT_ASSERT( blockOp.getBitFlag( val, 3 ) == 0 );

          blockOp.setBitFlag( &val, 2, true );
          CPPUNIT_ASSERT( blockOp.getBitFlag( val, 2 ) == true );
          CPPUNIT_ASSERT( val == 7 );

          blockOp.setBitFlag( &val, 1, false );
          CPPUNIT_ASSERT( blockOp.getBitFlag( val, 1 ) == false );
          CPPUNIT_ASSERT( val == 5 );
      }

      void testBufferBit()
      {
          BlockOp blockOp;
          unsigned char buf[BYTE_PER_BLOCK];

          blockOp.resetBuf( buf, sizeof( buf ) );

          blockOp.setBitFlag( (unsigned int*)&buf[1], 0, true );
          blockOp.setBitFlag( (unsigned int*)&buf[1], 3, true );

          blockOp.setBitFlag( (unsigned int*)&buf[6], 1, true );
          blockOp.setBitFlag( (unsigned int*)&buf[6], 2, true );
          blockOp.setBitFlag( (unsigned int*)&buf[6], 3, true );
          blockOp.setBitFlag( (unsigned int*)&buf[6], 5, true );

          CPPUNIT_ASSERT( blockOp.getBufTotalOnBit( buf, sizeof( buf )) == 6 );

          CPPUNIT_ASSERT( blockOp.getBufByteBitFlag( buf, 6, 5 ) == true );
          CPPUNIT_ASSERT( blockOp.getBufByteBitFlag( buf, 6, 3 ) == true );
          CPPUNIT_ASSERT( blockOp.getBufByteBitFlag( buf, 1, 0 ) == true );
          CPPUNIT_ASSERT( blockOp.getBufByteBitFlag( buf, 1, 3 ) == true );

          blockOp.setBufByteBitFlag( buf, 6, 2, false );
          CPPUNIT_ASSERT( blockOp.getBufByteBitFlag( buf, 6, 2 ) == false );
          CPPUNIT_ASSERT( blockOp.getBufTotalOnBit( buf, sizeof( buf )) == 5 );
      }

      void testBitShift()
      {
          BlockOp blockOp;
          uint64_t curVal, result, mask;

          curVal = 0x6a;
          mask = 0xe0; // first three bit
          result = blockOp.getBitValue( curVal, 5, 0xF );
          CPPUNIT_ASSERT( result == 3 );

          result = blockOp.setBitValue( curVal, 0x4, 5, mask );
          CPPUNIT_ASSERT( result == 138 );
      }

      void testEmptyRowValue( )
      {
          BlockOp  blockOp;
          uint64_t      curVal;
          RID      curRID;

          curRID = blockOp.getRowId( 0, 1, 3 );
          CPPUNIT_ASSERT( curRID == 3 );

          curRID = blockOp.getRowId( 0, 2, 3 );
          CPPUNIT_ASSERT( curRID == 3 );

          curRID = blockOp.getRowId( 2, 4, 3 );
          CPPUNIT_ASSERT( curRID == 4099 );

          curVal = blockOp.getEmptyRowValue( WriteEngine::TINYINT, 1 );
          CPPUNIT_ASSERT( curVal == 0x81 );

          curVal = blockOp.getEmptyRowValue( WriteEngine::SMALLINT, 2 );
          CPPUNIT_ASSERT( curVal == 0x8001 );

          curVal = blockOp.getEmptyRowValue( WriteEngine::MEDINT, 4 );
          CPPUNIT_ASSERT( curVal == 0x80000001 );

          curVal = blockOp.getEmptyRowValue( WriteEngine::INT, 4 );
          CPPUNIT_ASSERT( curVal == 0x80000001 );

          curVal = blockOp.getEmptyRowValue( WriteEngine::BIGINT, 8 );
          CPPUNIT_ASSERT( curVal == 0x8000000000000001LL );

          curVal = blockOp.getEmptyRowValue( WriteEngine::FLOAT, 4 );
          CPPUNIT_ASSERT( curVal == 0xFFAAAAAB );

          curVal = blockOp.getEmptyRowValue( WriteEngine::DOUBLE, 8 );
          CPPUNIT_ASSERT( curVal == 0xFFFAAAAAAAAAAAABLL );

          curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 1 );
          CPPUNIT_ASSERT( curVal == 0x8001 );

          curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 2 );
          CPPUNIT_ASSERT( curVal == 0x8001 );

          curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 3 );
          CPPUNIT_ASSERT( curVal == 0x80000001 );

          curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 4 );
          CPPUNIT_ASSERT( curVal == 0x80000001 );

          curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 5 );
          CPPUNIT_ASSERT( curVal == 0x8000000000000001LL );

          curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 6 );
          CPPUNIT_ASSERT( curVal == 0x8000000000000001LL );

          curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 7 );
          CPPUNIT_ASSERT( curVal == 0x8000000000000001LL );

          curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 8 );
          CPPUNIT_ASSERT( curVal == 0x8000000000000001LL );

  //              curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 9 );
  //              CPPUNIT_ASSERT( curVal == 0x80000001 );
  //
  //              curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 10 );
  //              CPPUNIT_ASSERT( curVal == 0x8000000000000001LL );
  //
  //              curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 12 );
  //              CPPUNIT_ASSERT( curVal == 0x8000000000000001LL );
  //
  //              curVal = blockOp.getEmptyRowValue( WriteEngine::DECIMAL, 19 );
  //              CPPUNIT_ASSERT( curVal == 0xFFFFFFFFFFFFFFFFLL );
  //
          curVal = blockOp.getEmptyRowValue( WriteEngine::DATE, 4 );
          CPPUNIT_ASSERT( curVal == 0xFFFFFFFF );

          curVal = blockOp.getEmptyRowValue( WriteEngine::DATETIME, 8 );
          CPPUNIT_ASSERT( curVal == 0xFFFFFFFFFFFFFFFFLL );

          curVal = blockOp.getEmptyRowValue( WriteEngine::CHAR, 1 );
          CPPUNIT_ASSERT( curVal == 0xFF );
          curVal = blockOp.getEmptyRowValue( WriteEngine::CHAR, 2 );
          CPPUNIT_ASSERT( curVal == 0xFFFF );
          curVal = blockOp.getEmptyRowValue( WriteEngine::CHAR, 4 );
          CPPUNIT_ASSERT( curVal == 0xFFFFFFFF );
          curVal = blockOp.getEmptyRowValue( WriteEngine::CHAR, 5 );
          CPPUNIT_ASSERT( curVal == 0xFFFFFFFFFFFFFFFFLL );
          curVal = blockOp.getEmptyRowValue( WriteEngine::CHAR, 8 );
          CPPUNIT_ASSERT( curVal == 0xFFFFFFFFFFFFFFFFLL );
          curVal = blockOp.getEmptyRowValue( WriteEngine::CHAR, 10 );
          CPPUNIT_ASSERT( curVal == 0xFFFFFFFFFFFFFFFFLL );

          curVal = blockOp.getEmptyRowValue( WriteEngine::VARCHAR, 1 );
          CPPUNIT_ASSERT( curVal == 0xFFFF );
          curVal = blockOp.getEmptyRowValue( WriteEngine::VARCHAR, 2 );
          CPPUNIT_ASSERT( curVal == 0xFFFFFFFF );
          curVal = blockOp.getEmptyRowValue( WriteEngine::VARCHAR, 3 );
          CPPUNIT_ASSERT( curVal == 0xFFFFFFFF );
          curVal = blockOp.getEmptyRowValue( WriteEngine::VARCHAR, 4 );
          CPPUNIT_ASSERT( curVal == 0xFFFFFFFFFFFFFFFFLL );
          curVal = blockOp.getEmptyRowValue( WriteEngine::VARCHAR, 5 );
          CPPUNIT_ASSERT( curVal == 0xFFFFFFFFFFFFFFFFLL );
          curVal = blockOp.getEmptyRowValue( WriteEngine::VARCHAR, 10 );
          CPPUNIT_ASSERT( curVal == 0xFFFFFFFFFFFFFFFFLL );
      }
      void testCorrectRowWidth()
      {
          BlockOp  blockOp;
          int      curVal;

          curVal = blockOp.getCorrectRowWidth( WriteEngine::TINYINT, 1 );
          CPPUNIT_ASSERT( curVal == 1 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::SMALLINT, 1 );  // intentionally put 1 rather than
  2 CPPUNIT_ASSERT( curVal == 2 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::MEDINT, 8 );   // intentionally put 8 rather than
  4 CPPUNIT_ASSERT( curVal == 4 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::INT, 4 );
          CPPUNIT_ASSERT( curVal == 4 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::BIGINT, 4 );  // should be 8
          CPPUNIT_ASSERT( curVal == 8 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::FLOAT, 8 ); // should be 4
          CPPUNIT_ASSERT( curVal == 4 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::DOUBLE, 4 ); // should be 8
          CPPUNIT_ASSERT( curVal == 8 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 1 );
          CPPUNIT_ASSERT( curVal == 2 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 2 );
          CPPUNIT_ASSERT( curVal == 2 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 3 );
          CPPUNIT_ASSERT( curVal == 4 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 4 );
          CPPUNIT_ASSERT( curVal == 4 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 5 );
          CPPUNIT_ASSERT( curVal == 8 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 6 );
          CPPUNIT_ASSERT( curVal == 8 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 7 );
          CPPUNIT_ASSERT( curVal == 8 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 8 );
          CPPUNIT_ASSERT( curVal == 8 );

  //              curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 9 );
  //              CPPUNIT_ASSERT( curVal == 4 );
  //
  //              curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 10 );
  //              CPPUNIT_ASSERT( curVal == 8 );
  //
  //              curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 12 );
  //              CPPUNIT_ASSERT( curVal == 8 );
  //
  //              curVal = blockOp.getCorrectRowWidth( WriteEngine::DECIMAL, 19 );
  //              CPPUNIT_ASSERT( curVal == 8 );
  //
          curVal = blockOp.getCorrectRowWidth( WriteEngine::DATE, 8 );
          CPPUNIT_ASSERT( curVal == 4 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::DATETIME, 4 );
          CPPUNIT_ASSERT( curVal == 8 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::CHAR, 1 );
          CPPUNIT_ASSERT( curVal == 1 );
          curVal = blockOp.getCorrectRowWidth( WriteEngine::CHAR, 2 );
          CPPUNIT_ASSERT( curVal == 2 );
          curVal = blockOp.getCorrectRowWidth( WriteEngine::CHAR, 3 );
          CPPUNIT_ASSERT( curVal == 4 );
          curVal = blockOp.getCorrectRowWidth( WriteEngine::CHAR, 5 );
          CPPUNIT_ASSERT( curVal == 8 );
          curVal = blockOp.getCorrectRowWidth( WriteEngine::CHAR, 8 );
          CPPUNIT_ASSERT( curVal == 8 );
          curVal = blockOp.getCorrectRowWidth( WriteEngine::CHAR, 10 );
          CPPUNIT_ASSERT( curVal == 8 );

          curVal = blockOp.getCorrectRowWidth( WriteEngine::VARCHAR, 1 );
          CPPUNIT_ASSERT( curVal == 2 );
          curVal = blockOp.getCorrectRowWidth( WriteEngine::VARCHAR, 2 );
          CPPUNIT_ASSERT( curVal == 4 );
          curVal = blockOp.getCorrectRowWidth( WriteEngine::VARCHAR, 3 );
          CPPUNIT_ASSERT( curVal == 4 );
          curVal = blockOp.getCorrectRowWidth( WriteEngine::VARCHAR, 4 );
          CPPUNIT_ASSERT( curVal == 8 );
          curVal = blockOp.getCorrectRowWidth( WriteEngine::VARCHAR, 5 );
          CPPUNIT_ASSERT( curVal == 8 );
          curVal = blockOp.getCorrectRowWidth( WriteEngine::VARCHAR, 10 );
          CPPUNIT_ASSERT( curVal == 8 );
      }


      void testDbBlock()
      {
          DbFileOp    dbFileOp;
          DataBlock   block;
          uint64_t    outVal = 12345, inVal;
          int         rc;
          FILE*       pFile;
          int         allocSize = 0;

          Stats::setUseStats( true );
          BRMWrapper::setUseBrm(true);
          dbFileOp.deleteFile( 999 );
          CPPUNIT_ASSERT( dbFileOp.exists( 999 ) == false );

          CPPUNIT_ASSERT( dbFileOp.createFile( 999, 10, allocSize ) == NO_ERROR );

          pFile = dbFileOp.openFile( 999 );
          CPPUNIT_ASSERT( pFile != NULL );

          uint64_t lbid0;
          BRMWrapper::getInstance()->getBrmInfo( 999, 0, lbid0 );
          rc = dbFileOp.readDBFile( pFile, block.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          dbFileOp.setSubBlockEntry( block.data, 1, 2, 8, &outVal );

          rc = dbFileOp.writeDBFile( pFile, block.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = dbFileOp.readSubBlockEntry( pFile, &block, lbid0, 1, 2, 8, &inVal );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          CPPUNIT_ASSERT( inVal == outVal );

          inVal = 0;
          dbFileOp.getSubBlockEntry( block.data, 1, 2, 8, &inVal );
          CPPUNIT_ASSERT( inVal == outVal );

          rc = dbFileOp.writeSubBlockEntry( pFile, &block, lbid0, 1, 2, 8, &inVal );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = dbFileOp.readDBFile( pFile, &block, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = dbFileOp.writeDBFile( pFile, &block, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          // Not Use Block resolution manager
          BRMWrapper::setUseBrm(false);
          dbFileOp.deleteFile( 999 );
          CPPUNIT_ASSERT( dbFileOp.exists( 999 ) == false );

          CPPUNIT_ASSERT( dbFileOp.createFile( 999, 10, allocSize ) == NO_ERROR );

          pFile = dbFileOp.openFile( 999 );
          CPPUNIT_ASSERT( pFile != NULL );

          rc = dbFileOp.readDBFile( pFile, block.data, 0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          dbFileOp.setSubBlockEntry( block.data, 1, 2, 8, &outVal );

          rc = dbFileOp.writeDBFile( pFile, block.data, 0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = dbFileOp.readSubBlockEntry( pFile, &block, 0, 1, 2, 8, &inVal );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          CPPUNIT_ASSERT( inVal == outVal );

          inVal = 0;
          dbFileOp.getSubBlockEntry( block.data, 1, 2, 8, &inVal );
          CPPUNIT_ASSERT( inVal == outVal );

          rc = dbFileOp.writeSubBlockEntry( pFile, &block, (uint64_t)0, 1, 2, 8, &inVal );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = dbFileOp.readDBFile( pFile, &block, 0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = dbFileOp.writeDBFile( pFile, &block, 0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          long readCounter, writeCounter;
          readCounter = Stats::getIoBlockRead();
          writeCounter = Stats::getIoBlockWrite();
          printf( "\nTotal number of blocks read: %ld", readCounter );
          printf( "\nTotal number of blocks write: %ld", writeCounter );
          Stats::setUseStats( false );
      }

      void testCopyDbFile()
      {
          DbFileOp    dbFileOp;
          int         rc, startFbo = 5, copyBlock = 15, bufBlock = 30, extendBlock;
          FILE*       pSourceFile = NULL;
          FILE*       pTargetFile = NULL;
          int         allocSize999 = 0, allocSize998 = 0;

          BRMWrapper::setUseBrm(true);

          rc = dbFileOp.copyFileValidation( pSourceFile, pTargetFile, startFbo,
                                            copyBlock, bufBlock, extendBlock );
          CPPUNIT_ASSERT( rc == ERR_FILE_NULL );

          dbFileOp.deleteFile( 999 );
          dbFileOp.deleteFile( 998 );
          CPPUNIT_ASSERT( dbFileOp.exists( 999 ) == false );
          CPPUNIT_ASSERT( dbFileOp.exists( 998 ) == false );

          CPPUNIT_ASSERT( dbFileOp.createFile( 999, 10, allocSize999 ) == NO_ERROR );
          CPPUNIT_ASSERT( dbFileOp.createFile( 998, 20, allocSize998, 0xFFFF, 2 ) == NO_ERROR );

          pTargetFile = dbFileOp.openFile( 999 );
          CPPUNIT_ASSERT( pTargetFile != NULL );

          pSourceFile = dbFileOp.openFile( 998 );
          CPPUNIT_ASSERT( pSourceFile != NULL );

  //      rc = dbFileOp.copyFileValidation( pSourceFile, pTargetFile, startFbo,
  //                                        copyBlock, bufBlock, extendBlock );
  //      CPPUNIT_ASSERT( rc == ERR_SIZE_NOT_MATCH );
          //CPPUNIT_ASSERT( rc == ERR_SIZE_NOT_MATCH );

          //copyBlock = 20;
          copyBlock = allocSize998;
          bufBlock = allocSize999;

          rc = dbFileOp.copyFileValidation( pSourceFile, pTargetFile, startFbo,
                                            copyBlock, bufBlock, extendBlock );
          CPPUNIT_ASSERT( rc == ERR_FILE_NEED_EXTEND );
          //CPPUNIT_ASSERT( extendBlock == 10245 );

          int allocExtSize999 = 0;
          rc = dbFileOp.extendFile( pTargetFile, 999, extendBlock, allocExtSize999, 0, 8 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = dbFileOp.copyFileValidation( pSourceFile, pTargetFile, startFbo,
                                            copyBlock, bufBlock, extendBlock );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          CPPUNIT_ASSERT( extendBlock == 0 );

          uint64_t lbidTarget;
          BRMWrapper::getInstance()->getBrmInfo( 999, startFbo, lbidTarget );
          rc = dbFileOp.copyDBFile( pSourceFile, pTargetFile, lbidTarget );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          dbFileOp.closeFile( pSourceFile );
          dbFileOp.closeFile( pTargetFile );

          //Not Use BRM
          BRMWrapper::setUseBrm(false);
          pSourceFile = NULL;
          pTargetFile = NULL;
          rc = dbFileOp.copyFileValidation( pSourceFile, pTargetFile, startFbo,
                                            copyBlock, bufBlock, extendBlock );
          CPPUNIT_ASSERT( rc == ERR_FILE_NULL );

          dbFileOp.deleteFile( 999 );
          dbFileOp.deleteFile( 998 );
          CPPUNIT_ASSERT( dbFileOp.exists( 999 ) == false );
          CPPUNIT_ASSERT( dbFileOp.exists( 998 ) == false );

          CPPUNIT_ASSERT( dbFileOp.createFile( 999, 10, allocSize999 ) == NO_ERROR );
          CPPUNIT_ASSERT( dbFileOp.createFile( 998, 20, allocSize998, 0xFFFF, 2 ) == NO_ERROR );

          pTargetFile = dbFileOp.openFile( 999 );
          CPPUNIT_ASSERT( pTargetFile != NULL );

          pSourceFile = dbFileOp.openFile( 998 );
          CPPUNIT_ASSERT( pSourceFile != NULL );

  //      rc = dbFileOp.copyFileValidation( pSourceFile, pTargetFile, startFbo,
  //                                        copyBlock, bufBlock, extendBlock );
  //      CPPUNIT_ASSERT( rc == ERR_SIZE_NOT_MATCH );

          //CPPUNIT_ASSERT( rc == ERR_SIZE_NOT_MATCH );
          //CPPUNIT_ASSERT( rc == NO_ERROR);

          copyBlock = 20;
          bufBlock  = 30;
          rc = dbFileOp.copyFileValidation( pSourceFile, pTargetFile, startFbo,
                                            copyBlock, bufBlock, extendBlock );
          CPPUNIT_ASSERT( rc == ERR_FILE_NEED_EXTEND );
          CPPUNIT_ASSERT( extendBlock == 45 );

          rc = dbFileOp.extendFile( pTargetFile, 999, extendBlock, 0, 8 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = dbFileOp.copyFileValidation( pSourceFile, pTargetFile, startFbo,
                                            copyBlock, bufBlock, extendBlock );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          CPPUNIT_ASSERT( extendBlock == 0 );

          rc = dbFileOp.copyDBFile( pSourceFile, pTargetFile, startFbo );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          dbFileOp.closeFile( pSourceFile );
          dbFileOp.closeFile( pTargetFile );

      }
  */

  void testExtensionWOPrealloc()
  {
    IDBDataFile* pFile = NULL;
    FileOp fileOp;
    BlockOp blockOp;
    char fileName[20];
    int rc;
    char hdrs[CompressInterface::HDR_BUF_LEN * 2];
    int dbRoot = 1;

    printf("\nRunning testExtensionWOPrealloc \n");
    idbdatafile::IDBPolicy::init(true, false, "", 0);
    // Set to versionbuffer to satisfy IDBPolicy::getType
    strcpy(fileName, "versionbuffer");
    fileOp.compressionType(1);

    fileOp.deleteFile(fileName);
    CPPUNIT_ASSERT(fileOp.exists(fileName) == false);

    int width = blockOp.getCorrectRowWidth(execplan::CalpontSystemCatalog::BIGINT, 8);
    int nBlocks = INITIAL_EXTENT_ROWS_TO_DISK / BYTE_PER_BLOCK * width;
    const uint8_t* emptyVal = blockOp.getEmptyRowValue(execplan::CalpontSystemCatalog::BIGINT, 8);
    // createFile runs IDBDataFile::open + initAbrevCompColumnExtent
    // under the hood
    // bigint column file
    rc = fileOp.createFile(fileName,
                           nBlocks,   // number of blocks
                           emptyVal,  // NULL value
                           width,     // width
                           execplan::CalpontSystemCatalog::BIGINT,
                           dbRoot);  // dbroot
    CPPUNIT_ASSERT(rc == NO_ERROR);

    // open created compressed file and check its header
    pFile = IDBDataFile::open(IDBPolicy::getType(fileName, IDBPolicy::WRITEENG), fileName, "rb", dbRoot);

    rc = pFile->seek(0, 0);
    CPPUNIT_ASSERT(rc == NO_ERROR);
    rc = fileOp.readHeaders(pFile, hdrs);
    CPPUNIT_ASSERT(rc == NO_ERROR);
    // Couldn't use IDBDataFile->close() here w/o excplicit cast
    fileOp.closeFile(pFile);

    // Extend the extent up to 64MB
    // first run w preallocation
    idbdatafile::BufferedFile* bFile = new idbdatafile::BufferedFile(fileName, "r+b", 0);
    pFile = dynamic_cast<IDBDataFile*>(bFile);

    rc = fileOp.initColumnExtent(pFile, dbRoot,
                                 BYTE_PER_BLOCK,  // number of blocks
                                 emptyVal, width, execplan::CalpontSystemCatalog::BIGINT,
                                 false,   // use existing file
                                 true,    // expand the extent
                                 false,   // add full (not abbreviated) extent
                                 false);  // don't optimize extention

    CPPUNIT_ASSERT(rc == NO_ERROR);
    CPPUNIT_ASSERT(bFile->size() == 2105344);
    fileOp.closeFile(pFile);
    // file has been extended delete the file before
    // the second run

    fileOp.deleteFile(fileName);
    CPPUNIT_ASSERT(fileOp.exists(fileName) == false);

    // second run with disabled preallocation
    rc = fileOp.createFile(fileName,
                           nBlocks,   // number of blocks
                           emptyVal,  // NULL value
                           width,     // width
                           execplan::CalpontSystemCatalog::BIGINT,
                           dbRoot);  // dbroot
    CPPUNIT_ASSERT(rc == NO_ERROR);

    // open created compressed file and check its header
    pFile = IDBDataFile::open(IDBPolicy::getType(fileName, IDBPolicy::WRITEENG), fileName, "rb", dbRoot);

    rc = pFile->seek(0, 0);
    CPPUNIT_ASSERT(rc == NO_ERROR);
    rc = fileOp.readHeaders(pFile, hdrs);
    CPPUNIT_ASSERT(rc == NO_ERROR);
    fileOp.closeFile(pFile);

    bFile = new idbdatafile::BufferedFile(fileName, "r+b", 0);
    pFile = dynamic_cast<IDBDataFile*>(bFile);

    // enable disk space preallocation and extend
    idbdatafile::IDBPolicy::enablePreallocSpace(dbRoot);
    rc = fileOp.initColumnExtent(pFile, dbRoot,
                                 BYTE_PER_BLOCK,  // number of blocks
                                 emptyVal, width, execplan::CalpontSystemCatalog::BIGINT,
                                 false,  // use existing file
                                 true,   // expand the extent
                                 false,  // add full (not abbreviated) extent
                                 true);  // optimize extention

    CPPUNIT_ASSERT(rc == NO_ERROR);
    CPPUNIT_ASSERT(bFile->size() == 67108864);
    fileOp.closeFile(pFile);
    // file has been extended

    fileOp.deleteFile(fileName);
    CPPUNIT_ASSERT(fileOp.exists(fileName) == false);
  }

  // Create a dict file. Extend it w and w/o preallocation.
  // Check the file sizes.
  void testDictExtensionWOPrealloc()
  {
    FileOp fileOp;
    BlockOp blockOp;
    char fileName[20];
    int rc;
    int dbRoot = 1;
    int colWidth = 65535;

    DctnryCompress1 m_Dctnry(/*compressionType=*/1);
    // This is the magic for the stub in FileOp::oid2FileName
    int oId = 42;

    printf("\nRunning testDictExtensionWOPrealloc ");
    printf("There could be InetStreamSocket::connect errors \n");
    m_Dctnry.setDebugLevel(DEBUG_3);

    idbdatafile::IDBPolicy::init(true, false, "", 0);
    // Set to versionbuffer to satisfy IDBPolicy::getType
    strcpy(fileName, "versionbuffer");

    rc = m_Dctnry.dropDctnry(oId);
    // FileOp::oid2FileName is called under the hood
    // Dctnry::createDctnry could be used with running CS
    // createDctnryFile also uses DBRM under the hood it works though.
    IDBDataFile* m_dFile = m_Dctnry.createDctnryFileUnit(fileName, colWidth, "w+b", DEFAULT_BUFSIZ);

    idbdatafile::BufferedFile* bFile = (idbdatafile::BufferedFile*)m_dFile;
    CPPUNIT_ASSERT(m_dFile != NULL);

    const int m_totalHdrBytes = HDR_UNIT_SIZE + NEXT_PTR_BYTES + HDR_UNIT_SIZE + HDR_UNIT_SIZE;

    m_Dctnry.compressionType(1);
    rc = m_Dctnry.initDctnryExtent(m_dFile, dbRoot,
                                   BYTE_PER_BLOCK,  // 8192
                                   const_cast<unsigned char*>(m_Dctnry.getDctnryHeader2()), m_totalHdrBytes,
                                   false,
                                   false);  // enable preallocation
    // Check the file size and remove the file
    CPPUNIT_ASSERT(bFile->size() == 483328);
    CPPUNIT_ASSERT(rc == NO_ERROR);
    fileOp.deleteFile(fileName);
    CPPUNIT_ASSERT(fileOp.exists(fileName) == false);

    // Create a Dictionary for the second time
    m_dFile = m_Dctnry.createDctnryFileUnit(fileName, colWidth, "w+b", DEFAULT_BUFSIZ);

    // Get the file size later
    bFile = (idbdatafile::BufferedFile*)m_dFile;
    CPPUNIT_ASSERT(m_dFile != NULL);

    // enable preallocation and create a Dictionary
    idbdatafile::IDBPolicy::enablePreallocSpace(dbRoot);
    m_Dctnry.compressionType(1);
    rc = m_Dctnry.initDctnryExtent(m_dFile, dbRoot, BYTE_PER_BLOCK,
                                   const_cast<unsigned char*>(m_Dctnry.getDctnryHeader2()), m_totalHdrBytes,
                                   false,
                                   true);  // skip preallocation

    // Check the size and remove the file.
    CPPUNIT_ASSERT(bFile->size() == 67379200);
    CPPUNIT_ASSERT(rc == NO_ERROR);
    fileOp.deleteFile(fileName);
    CPPUNIT_ASSERT(fileOp.exists(fileName) == false);
  }

  void testLog()
  {
    Log log;
    FileOp fileOp;
    string msg;
    int iVal = 3;
    char logFile[] = "test1.log";
    char logErrFile[] = "test1err.log";

    log.setLogFileName(logFile, logErrFile);

    msg = Convertor::int2Str(iVal);
    log.logMsg(msg + " this is a info message", MSGLVL_INFO1);
    msg = Convertor::getTimeStr();
    log.logMsg(" this is a warning message", MSGLVL_WARNING);
    log.logMsg("this is an error message ", 1011, MSGLVL_ERROR);
    log.logMsg("this is a critical message", 1211, MSGLVL_CRITICAL);
    CPPUNIT_ASSERT(fileOp.exists(logFile) == true);
    CPPUNIT_ASSERT(fileOp.exists(logErrFile) == true);
    fileOp.deleteFile(logFile);
    fileOp.deleteFile(logErrFile);
    CPPUNIT_ASSERT(fileOp.exists(logFile) == false);
    CPPUNIT_ASSERT(fileOp.exists(logErrFile) == false);
  }

  /*

      void testHWM()
      {
          int rc ;
          int hwm = 0;
          int allocSize = 0, allocSize1 = 0;
          OID oid  = 999;
          DbFileOp    dbFileOp;
          VER_t       transId = 11;

          BRMWrapper::setUseBrm(true);
          BRMWrapper::getInstance()->rollBack( transId );

          // create something to increase LBID number
          dbFileOp.deleteFile( oid - 1 );
          rc  = dbFileOp.createFile( oid - 1, 10, allocSize1 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          dbFileOp.deleteFile( oid );
          CPPUNIT_ASSERT( dbFileOp.exists( oid ) == false );

          CPPUNIT_ASSERT( dbFileOp.createFile( oid, 10, allocSize1 ) == NO_ERROR );

          rc = BRMWrapper::getInstance()->getHWM( oid, hwm);
          CPPUNIT_ASSERT(  rc == NO_ERROR );
          hwm = 10;
          rc = BRMWrapper::getInstance()->setHWM( oid,  hwm );
          CPPUNIT_ASSERT(  rc == NO_ERROR );
          rc = BRMWrapper::getInstance()->getHWM( oid, hwm);
          CPPUNIT_ASSERT(  rc == NO_ERROR );
          CPPUNIT_ASSERT(  hwm == 10 );

          rc = BRMWrapper::getInstance()->allocateExtent( oid, -1, allocSize);
          CPPUNIT_ASSERT(  rc != NO_ERROR );
          rc = BRMWrapper::getInstance()->allocateExtent( oid, 20, allocSize);
          CPPUNIT_ASSERT(  rc == NO_ERROR );
          CPPUNIT_ASSERT(  allocSize > 0 );
      }

      void testVB()
      {
          FileOp                     fileOp;
          FILE*                      pFile;
          OID                        oid = 999;
          LBIDRange                  range;
          vector<struct LBIDRange>   rangeList;
          vector<uint32_t>           fboList;
          uint64_t                   curLbid;
          int                        rc;
          VER_t                      transId = 11;

          BRMWrapper::getInstance()->setDebugLevel( DEBUG_3 );

          // change fbo 3
          rc = BRMWrapper::getInstance()->getBrmInfo( oid, 3, curLbid );
          CPPUNIT_ASSERT(  rc == NO_ERROR );
          CPPUNIT_ASSERT(  curLbid >= 0 );

          range.start = 0;
          range.size = 1;

          rangeList.push_back( range );
          printf( "\ncur lbid=%ld", (long)curLbid );

          pFile = fileOp.openFile( oid );
          CPPUNIT_ASSERT(  pFile != NULL );

          fboList.push_back( 3 );
          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, fboList, rangeList );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, fboList, rangeList );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          // commit
          rc = BRMWrapper::getInstance()->commit( transId );
          CPPUNIT_ASSERT(  rc == NO_ERROR );


          // write to the same block
          transId++;
          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, fboList, rangeList );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          // commit
          rc = BRMWrapper::getInstance()->commit( transId );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          // the same block should be locked by BRM
          transId++;
          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, fboList, rangeList );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          transId++;
          // rollback
          fboList[0] += 1;
          rangeList[0].start += 1;
          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, fboList, rangeList );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          rc = BRMWrapper::getInstance()->commit( transId );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          transId++;
          // write lbid
          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, curLbid + 5);
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, curLbid + 6);
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          rc = BRMWrapper::getInstance()->commit( transId );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          fileOp.closeFile( pFile );

          // test rollback whether can write back the original value after three insert
          printf( "\n\nThis is the test for rollback after three insert for the same trans\n" );
          DbFileOp    dbFileOp;
          DataBlock   block, originBlock;
          uint64_t         lbid0;

          pFile = dbFileOp.openFile( oid );
          CPPUNIT_ASSERT( pFile != NULL );

          BRMWrapper::getInstance()->getBrmInfo( oid, 10, lbid0 );
          memset( block.data, 'a', BYTE_PER_BLOCK );
          rc = dbFileOp.writeDBFile( pFile, block.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = dbFileOp.readDBFile( pFile, originBlock.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );


          transId++;
          fboList.clear();
          fboList.push_back( 10 );

          // first time
          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, fboList );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          memset( block.data, 'b', BYTE_PER_BLOCK );
          rc = dbFileOp.writeDBFile( pFile, block.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          // second time
          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, fboList );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          memset( block.data, 'c', BYTE_PER_BLOCK );
          rc = dbFileOp.writeDBFile( pFile, block.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          // third time
          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, fboList );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          memset( block.data, 'd', BYTE_PER_BLOCK );
          rc = dbFileOp.writeDBFile( pFile, block.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          fflush(pFile);
          rc = BRMWrapper::getInstance()->rollBack( transId );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          fflush(pFile);
          rc = dbFileOp.readDBFile( pFile, block.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          CPPUNIT_ASSERT( memcmp( block.data, originBlock.data, BYTE_PER_BLOCK ) == 0 );

          // test commit twice and rollback to see whether rollback commit values
          printf( "\n\nThis is the test for commit twice and then rollback to see whether the previous two
  committed one got rollback\n" );

          // first commit
          transId++;
          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, fboList );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          memset( block.data, 'e', BYTE_PER_BLOCK );
          rc = dbFileOp.writeDBFile( pFile, block.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = BRMWrapper::getInstance()->commit( transId );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          // second commit
          transId++;
          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, fboList );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          memset( block.data, 'f', BYTE_PER_BLOCK );
          rc = dbFileOp.writeDBFile( pFile, block.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          fflush(pFile);

          rc = BRMWrapper::getInstance()->commit( transId );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          memcpy( originBlock.data, block.data, BYTE_PER_BLOCK );

          // rollback
          transId++;
          rc = BRMWrapper::getInstance()->writeVB( pFile, transId, oid, fboList );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          memset( block.data, 'g', BYTE_PER_BLOCK );
          rc = dbFileOp.writeDBFile( pFile, block.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          fflush(pFile);

          rc = BRMWrapper::getInstance()->rollBack( transId );
          CPPUNIT_ASSERT(  rc == NO_ERROR );

          // check whether they are the same one
          rc = dbFileOp.readDBFile( pFile, block.data, lbid0 );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          fflush(pFile);

          CPPUNIT_ASSERT( memcmp( block.data, originBlock.data, BYTE_PER_BLOCK ) == 0 );
      }

      void testDM()
      {
          FileOp   fileOp;
          char     fileName[FILE_NAME_SIZE];
          int      rc;

          rc = fileOp.oid2FileName( 100, fileName, true );
          CPPUNIT_ASSERT( rc == NO_ERROR );

          rc = fileOp.oid2FileName( 3071, fileName, true );
          CPPUNIT_ASSERT( rc == NO_ERROR );


      }

      void testCacheBasic()
      {
          DataBlock block;
          OID       oid = 15;
          uint64_t  lbid = 1234, fbo = 3;
          int       rc;
          CacheKey  key;
          CommBlock cb;

          Cache::init();
          cb.file.oid = oid;
          memset( block.data, 'a', BYTE_PER_BLOCK );
          rc = Cache::insertLRUList( cb, lbid, fbo, block );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          CPPUNIT_ASSERT( Cache::cacheKeyExist( oid, lbid ) == true );

          // add a second block
          block.data[1] = '1';
          lbid = 1235;
          rc = Cache::insertLRUList( cb, lbid, fbo, block );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          CPPUNIT_ASSERT( Cache::cacheKeyExist( oid, lbid ) == true );

          // add one more block
          block.data[1] = '2';
          lbid = 1237;
          rc = Cache::insertLRUList( cb, lbid, fbo, block );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          CPPUNIT_ASSERT( Cache::cacheKeyExist( oid, lbid ) == true );

          // load one from lruList
          memset( block.data, 0, BYTE_PER_BLOCK );
          lbid = 1235;
          key = Cache::getCacheKey( oid, lbid );
          rc = Cache::loadCacheBlock( key, block );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          CPPUNIT_ASSERT( block.data[1] == '1' );
          CPPUNIT_ASSERT( block.data[2] == 'a' );

          // move one block from lrulist to write list
          block.data[2] = 'm';
          rc = Cache::modifyCacheBlock( key, block );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          memset( block.data, 0, BYTE_PER_BLOCK );  // clear
          rc = Cache::loadCacheBlock( key, block );
          CPPUNIT_ASSERT( rc == NO_ERROR );
          CPPUNIT_ASSERT( block.data[2] == 'm' );

          Cache::printCacheList();

          // flush cache
          Cache::flushCache();
          Cache::printCacheList();

          // shutdown
  //      Cache::freeMemory();
      }

      void testCacheReadWrite()
      {
          DbFileOp  dbFileOp;
          DataBlock block;
          CommBlock cb;
          OID       oid = 16;
          uint64_t  lbid = 1234;
          int       rc, totalBlock;
          int       allocSize = 0;

          BRMWrapper::setUseBrm(true);
          dbFileOp.setTransId( 3 );
          Cache::clear();

          cb.session.txnid = 10;
          cb.file.oid = oid;

          dbFileOp.deleteFile( oid );
          CPPUNIT_ASSERT( dbFileOp.exists( oid ) == false );

          CPPUNIT_ASSERT( dbFileOp.createFile( oid, 10, allocSize ) == NO_ERROR );

          cb.file.pFile = dbFileOp.openFile( oid );
          CPPUNIT_ASSERT( cb.file.pFile != NULL );

          BRMWrapper::getInstance()->getBrmInfo( oid, 3, lbid );

          Cache::setUseCache( true );
          Cache::clear();
          int size;

          size = Cache::getListSize( FREE_LIST );
          CPPUNIT_ASSERT( size == DEFAULT_CACHE_BLOCK );
          size = Cache::getListSize( WRITE_LIST );
          CPPUNIT_ASSERT( size == 0 );


          totalBlock = Cache::getTotalBlock();

          for ( long i = 0; i < totalBlock; i++ )
          {

              rc = dbFileOp.readDBFile( cb, block.data, lbid );
              CPPUNIT_ASSERT( rc == NO_ERROR );

              block.data[1] = 'k';
              rc = dbFileOp.writeDBFile( cb, block.data, lbid );
              CPPUNIT_ASSERT( rc == NO_ERROR );

              lbid++;

              if ( i == DEFAULT_CACHE_BLOCK - 20 )
                  BRMWrapper::getInstance()->getBrmInfo( oid, 3, lbid );

              if ( i % 5000 == 0 && i != 0 )
              {
                  printf( "\ni=%ld", i );
                  Cache::printCacheList();
              }

              if ( i == totalBlock - 2 )
              {
                  dbFileOp.flushCache();
                  Cache::setUseCache( false );
              }
          }

      }

      template<uint8_t W> struct binary;
      typedef binary<16> binary16;
      typedef binary<32> binary32;
      template<uint8_t W>
      struct binary {
          unsigned char data[W]; // May be ok for empty value ?
          void operator=(uint64_t v) {*((uint64_t *) data) = v; memset(data + 8, 0, W - 8);}
          inline uint8_t& operator[](const int index) {return *((uint8_t*) (data + index));}
          inline uint64_t& uint64(const int index) {return *((uint64_t*) (data + (index << 3)));}
      };

      void testExtentCrWOPreallocBin() {
          IDBDataFile* pFile = NULL;
          ColumnOpCompress1 fileOp;
          BlockOp blockOp;
          char fileName[20];
          int rc;
          char hdrs[ CompressInterface::HDR_BUF_LEN * 2 ];
          int dbRoot = 1;

          idbdatafile::IDBPolicy::init(true, false, "", 0);
          // Set to versionbuffer to satisfy IDBPolicy::getType
          strcpy(fileName, "versionbuffer");
          fileOp.compressionType(1);

          fileOp.deleteFile(fileName);
          CPPUNIT_ASSERT(fileOp.exists(fileName) == false);

          uint64_t emptyVal = blockOp.getEmptyRowValue(execplan::CalpontSystemCatalog::BIGINT, 8);
          int width = blockOp.getCorrectRowWidth(execplan::CalpontSystemCatalog::BINARY, sizeof (binary16));
          int nBlocks = INITIAL_EXTENT_ROWS_TO_DISK / BYTE_PER_BLOCK * width;

          // createFile runs IDBDataFile::open + initAbrevCompColumnExtent
          // under the hood
          // bigint column file
          rc = fileOp.createFile(fileName,
                  nBlocks, // number of blocks
                  emptyVal, // NULL value
                  width, // width
                  dbRoot); // dbroot
          CPPUNIT_ASSERT(rc == NO_ERROR);

          fileOp.closeFile(pFile);

          // open created compressed file and check its header
          pFile = IDBDataFile::open(IDBPolicy::getType(fileName,
                  IDBPolicy::WRITEENG), fileName, "rb", dbRoot);

          rc = pFile->seek(0, 0);
          CPPUNIT_ASSERT(rc == NO_ERROR);
          rc = fileOp.readHeaders(pFile, hdrs);
          CPPUNIT_ASSERT(rc == NO_ERROR);
          // Couldn't use IDBDataFile->close() here w/o excplicit cast
          fileOp.closeFile(pFile);

          // Extend the extent up to 64MB
          pFile = IDBDataFile::open(IDBPolicy::getType(fileName,
                  IDBPolicy::WRITEENG), fileName, "rb", dbRoot);

          // disable disk space preallocation
          idbdatafile::IDBPolicy::setPreallocSpace(dbRoot);
          rc = fileOp.initColumnExtent(pFile,
                  dbRoot,
                  BYTE_PER_BLOCK - nBlocks, // number of blocks
                  emptyVal,
                  width,
                  false, // use existing file
                  false, // don't expand; new extent
                  false, // add full (not abbreviated) extent
                  true); // optimize extention

          CPPUNIT_ASSERT(rc == NO_ERROR);
          fileOp.closeFile(pFile);
          // file has been extended
          cout << endl << "file has been extended";

          // write up to INITIAL_EXTENT_ROWS_TO_DISK + 1 rows into the file

          Column curCol;
          binary16 valArray[INITIAL_EXTENT_ROWS_TO_DISK + 1];
          RID rowIdArray[INITIAL_EXTENT_ROWS_TO_DISK + 1];
          // This is the magic for the stub in FileOp::oid2FileName
          int fid = 42;

          for (uint64_t it = 0; it <= INITIAL_EXTENT_ROWS_TO_DISK; it++) {
              rowIdArray[it] = it;
              valArray[it].uint64(0) = it + 3;
              valArray[it].uint64(1) = it + 5;
          }

          fileOp.initColumn(curCol);
          fileOp.setColParam(curCol,
                  1, // column number
                  width,
                  execplan::CalpontSystemCatalog::BINARY,
                  WriteEngine::WR_BINARY,
                  fid,
                  1); //compression type

          string segFile;
          // openColumnFile uses DBRM's oid server but we
          // have to get the chunks' pointers from the header.
          curCol.dataFile.pFile = fileOp.openFile(
                  curCol,
                  dbRoot,
                  0,
                  0,
                  segFile,
                  false,
                  "r+b",
                  BYTE_PER_BLOCK * BYTE_PER_BLOCK); // buffer size is 64MB

          CPPUNIT_ASSERT(rc == NO_ERROR);

          rc = fileOp.writeRow(curCol, INITIAL_EXTENT_ROWS_TO_DISK + 1,
                  (RID*) rowIdArray, valArray);
          CPPUNIT_ASSERT_EQUAL(NO_ERROR, rc); // I prefer this way as it prints values

          // flush and close the file used for reading
          fileOp.clearColumn(curCol);

          std::map<uint32_t, uint32_t> oids;
          oids[fid] = fid;

          // flush changed chunks from the Manager
          int rtn1 = fileOp.chunkManager()->flushChunks(rc, oids);

          // read back the file
          cout << endl << "Read file      ";
          DataBlock block;
          binary16* bin16 = (binary16*) block.data;

          fileOp.initColumn(curCol);
          fileOp.setColParam(curCol,
                  1, // column number
                  width,
                  execplan::CalpontSystemCatalog::BINARY,
                  WriteEngine::WR_BINARY,
                  fid,
                  1); //compression type

          curCol.dataFile.pFile = fileOp.openFile(
                  curCol,
                  dbRoot,
                  0,
                  0,
                  segFile,
                  false,
                  "r+b",
                  BYTE_PER_BLOCK * BYTE_PER_BLOCK); // buffer size is 64MB

          CPPUNIT_ASSERT_EQUAL(NO_ERROR, rc);

          int blocks = fileOp.blocksInFile(curCol.dataFile.pFile);

          for (int b = 0; b < blocks; b++) {
              rc = fileOp.chunkManager()->readBlock(curCol.dataFile.pFile, block.data, b); //
  ColumnOpCompress1.readBlock() is protected so ... CPPUNIT_ASSERT_EQUAL(NO_ERROR, rc);
              //cout << endl << bin16[0].uint64(0);
              CPPUNIT_ASSERT_EQUAL(b * 512UL + 3, bin16[0].uint64(0)); // Checking just first value of each
  block as it was written before CPPUNIT_ASSERT_EQUAL(b * 512UL + 5, bin16[0].uint64(1));
          }

          fileOp.clearColumn(curCol);
          fileOp.closeFile(curCol.dataFile.pFile); // Seems done by clearColumn, but anyways...

          cout << endl << "Delete file      ";

          fileOp.deleteFile(fileName);
          CPPUNIT_ASSERT(fileOp.exists(fileName) == false);
          cout << endl << "End of test";
      }
      */

  void testCleanup()
  {
    // shutdown
    Cache::freeMemory();
  }
};

CPPUNIT_TEST_SUITE_REGISTRATION(SharedTest);

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main(int argc, char** argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry& registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest(registry.makeTest());
  bool wasSuccessful = runner.run("", false);
  return (wasSuccessful ? 0 : 1);
}
