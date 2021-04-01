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

/******************************************************************************************
* $Id: idbcompress.cpp 3907 2013-06-18 13:32:46Z dcathey $
*
******************************************************************************************/
#include <cstring>
#include <iostream>
#include <stdexcept>
using namespace std;

#include "blocksize.h"
#include "logger.h"
#include "snappy.h"
#include "hasher.h"

#define IDBCOMP_DLLEXPORT
#include "idbcompress.h"
#undef IDBCOMP_DLLEXPORT

namespace
{
const uint64_t MAGIC_NUMBER = 0xfdc119a384d0778eULL;
const uint64_t VERSION_NUM1 = 1;
const uint64_t VERSION_NUM2 = 2;
const int      COMPRESSED_CHUNK_INCREMENT_SIZE = 8192;
const int      PTR_SECTION_OFFSET = compress::CompressInterface::HDR_BUF_LEN;

// version 1.1 of the chunk data has a short header
// QuickLZ compressed data never has the high bit set on the first byte
const uint8_t CHUNK_MAGIC1 = 0xff;
const int SIG_OFFSET = 0;
const int CHECKSUM_OFFSET = 1;
const int LEN_OFFSET = 5;
const unsigned HEADER_SIZE = 9;

/* version 1.2 of the chunk data changes the hash function used to calculate
 * checksums.  We can no longer use the algorithm used in ver 1.1.  Everything
 * else is the same
 */
const uint8_t CHUNK_MAGIC2 = 0xfe;

/* version 2.0 of the chunk data uses a new compression algo. For us, because of
 * the finite number of block sizes we compress, the first byte of the compressed
 * data will always be 0x80, so it can't be confused with V1.0 data (that has no
 * header).
 */
const uint8_t CHUNK_MAGIC3 = 0xfd;

struct CompressedDBFileHeader
{
    uint64_t fMagicNumber;
    uint64_t fVersionNum;
    uint64_t fCompressionType;
    uint64_t fHeaderSize;
    uint64_t fBlockCount;
    uint64_t fColumnWidth;
    execplan::CalpontSystemCatalog::ColDataType fColDataType;
};

// Make the header to be 4K, regardless number of fields being defined/used in header.
union CompressedDBFileHeaderBlock
{
    CompressedDBFileHeader fHeader;
    char fDummy[compress::CompressInterface::HDR_BUF_LEN];
};

void initCompressedDBFileHeader(void* hdrBuf, int compressionType, int hdrSize)
{
    CompressedDBFileHeaderBlock* hdr = reinterpret_cast<CompressedDBFileHeaderBlock*>(hdrBuf);
    hdr->fHeader.fMagicNumber     = MAGIC_NUMBER;
    hdr->fHeader.fVersionNum      = VERSION_NUM2;
    hdr->fHeader.fCompressionType = compressionType;
    hdr->fHeader.fBlockCount      = 0;
    hdr->fHeader.fHeaderSize      = hdrSize;
    hdr->fHeader.fColumnWidth     = 0;
    hdr->fHeader.fColDataType     = execplan::CalpontSystemCatalog::ColDataType::UNDEFINED;
}

void initCompressedDBFileHeader(
    void* hdrBuf, uint32_t columnWidth,
    execplan::CalpontSystemCatalog::ColDataType colDataType,
    int compressionType, int hdrSize)
{
    CompressedDBFileHeaderBlock* hdr = reinterpret_cast<CompressedDBFileHeaderBlock*>(hdrBuf);
    hdr->fHeader.fMagicNumber     = MAGIC_NUMBER;
    hdr->fHeader.fVersionNum      = VERSION_NUM2;
    hdr->fHeader.fCompressionType = compressionType;
    hdr->fHeader.fBlockCount      = 0;
    hdr->fHeader.fHeaderSize      = hdrSize;
    hdr->fHeader.fColumnWidth     = columnWidth;
    hdr->fHeader.fColDataType     = colDataType;
}

} // namespace


namespace compress
{
#ifndef SKIP_IDB_COMPRESSION

CompressInterface::CompressInterface(unsigned int numUserPaddingBytes) :
    fNumUserPaddingBytes(numUserPaddingBytes)
{ }

/* V1 is really only available for decompression, we kill any DDL using V1 by hand.
 * Maybe should have a new api, isDecompressionAvail() ? Any request to compress
 * using V1 will silently be changed to V2.
*/
/*static*/
bool CompressInterface::isCompressionAvail(int compressionType)
{
    if ((compressionType == 0) || (compressionType == 1) ||
        (compressionType == 2))
        return true;

    return false;
}

//------------------------------------------------------------------------------
// Compress a block of data
//------------------------------------------------------------------------------
int CompressInterface::compressBlock(const char* in, const size_t inLen,
                                        unsigned char* out,
                                        size_t& outLen) const
{
    size_t snaplen = 0;
    utils::Hasher128 hasher;

    // loose input checking.
    if (outLen < maxCompressedSize(inLen) + HEADER_SIZE)
    {
        cerr << "got outLen = " << outLen << " for inLen = " << inLen
             << ", needed " << (maxCompressedSize(inLen) + HEADER_SIZE)
             << endl;
        return ERR_BADOUTSIZE;
    }

    auto rc = compress(in, inLen, reinterpret_cast<char*>(&out[HEADER_SIZE]),
                       &snaplen);
    if (rc != ERR_OK)
    {
        return rc;
    }

    uint8_t* signature = (uint8_t*) &out[SIG_OFFSET];
    uint32_t* checksum = (uint32_t*) &out[CHECKSUM_OFFSET];
    uint32_t* len = (uint32_t*) &out[LEN_OFFSET];
    *signature = getChunkMagicNumber();
    *checksum = hasher((char*) &out[HEADER_SIZE], snaplen);
    *len = snaplen;

    //cerr << "cb: " << inLen << '/' << outLen << '/' << (snappy::MaxCompressedLength(inLen) + HEADER_SIZE) <<
    //	" : " << (snaplen + HEADER_SIZE) << endl;

    outLen = snaplen + HEADER_SIZE;

    return ERR_OK;
}

//------------------------------------------------------------------------------
// Decompress a block of data
//------------------------------------------------------------------------------
int CompressInterface::uncompressBlock(const char* in, const size_t inLen,
                                       unsigned char* out,
                                       size_t& outLen) const
{
    size_t ol = 0;

    uint32_t realChecksum;
    uint32_t storedChecksum;
    uint32_t storedLen;
    uint8_t storedMagic;
    utils::Hasher128 hasher;


    if (inLen < 1)
    {
        outLen = 0;
        return ERR_BADINPUT;
    }

    storedMagic = *((uint8_t*) &in[SIG_OFFSET]);

    if (storedMagic == getChunkMagicNumber())
    {
        if (inLen < HEADER_SIZE)
        {
            outLen = 0;
            return ERR_BADINPUT;
        }

        storedChecksum = *((uint32_t*) &in[CHECKSUM_OFFSET]);
        storedLen = *((uint32_t*) (&in[LEN_OFFSET]));

        if (inLen < storedLen + HEADER_SIZE)
        {
            outLen = 0;
            return ERR_BADINPUT;
        }

        realChecksum = hasher(&in[HEADER_SIZE], storedLen);

        if (storedChecksum != realChecksum)
        {
            outLen = 0;
            return ERR_CHECKSUM;
        }

        auto rc = uncompress(&in[HEADER_SIZE], storedLen,
                             reinterpret_cast<char*>(out), &outLen);
        if (rc != ERR_OK)
        {
            outLen = 0;
            cerr << "uncompressBlock failed!" << endl;
            return ERR_DECOMPRESS;
        }
    }
    else
    {
        // v1 compression or bad header
        outLen = 0;
        return ERR_BADINPUT;
    }

    //cerr << "ub: " << inLen << " : " << outLen << endl;

    return ERR_OK;
}

//------------------------------------------------------------------------------
// Verify the passed in buffer contains a valid compression file header.
//------------------------------------------------------------------------------
int CompressInterface::verifyHdr(const void* hdrBuf)
{
    const CompressedDBFileHeader* hdr = reinterpret_cast<const CompressedDBFileHeader*>(hdrBuf);

    if (hdr->fMagicNumber != MAGIC_NUMBER)
        return -1;

    if (!isCompressionAvail(hdr->fCompressionType))
        return -2;

    return 0;
}

//------------------------------------------------------------------------------
// Extract compression pointer information out of the pointer buffer that is
// passed in.  ptrBuf points to the pointer section of the compression hdr.
//------------------------------------------------------------------------------
int CompressInterface::getPtrList(const char* ptrBuf, const int ptrBufSize,
                                  CompChunkPtrList& chunkPtrs)
{
    int rc = 0;
    chunkPtrs.clear();

    const uint64_t* ptrs = reinterpret_cast<const uint64_t*>(ptrBuf);
    const unsigned int NUM_PTRS = ptrBufSize / sizeof(uint64_t);

    for (unsigned int i = 0; (i < NUM_PTRS) && (rc == 0); i++)
    {
        if (ptrs[i + 1] == 0) // 0 offset means end of data
            break;

        if (ptrs[i + 1] > ptrs[i])
            chunkPtrs.push_back(make_pair( ptrs[i], (ptrs[i + 1] - ptrs[i])));
        else
            rc = -1;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Extract compression pointer information out of the file compression hdr.
// Function assume that the file is a column file that has just two 4096-hdrs,
// one for the file header, and one for the list of pointers.
// Wrapper of above method for backward compatibility.
//------------------------------------------------------------------------------
int CompressInterface::getPtrList(const char* hdrBuf, CompChunkPtrList& chunkPtrs )
{
    return getPtrList(hdrBuf + HDR_BUF_LEN, HDR_BUF_LEN, chunkPtrs);
}

//------------------------------------------------------------------------------
// Count the number of chunk pointers in the pointer header(s)
//------------------------------------------------------------------------------
unsigned int CompressInterface::getPtrCount(const char* ptrBuf,
                                            const int ptrBufSize)
{
    unsigned int chunkCount = 0;

    const uint64_t* ptrs = reinterpret_cast<const uint64_t*>(ptrBuf);
    const unsigned int NUM_PTRS = ptrBufSize / sizeof(uint64_t);

    for (unsigned int i = 0; i < NUM_PTRS; i++)
    {
        if (ptrs[i + 1] == 0) // 0 offset means end of data
            break;

        chunkCount++;
    }

    return chunkCount;
}

//------------------------------------------------------------------------------
// Count the number of chunk pointers in the specified 8192 byte compression
// file header, which carries a single 4096 byte compression chunk header.
// This should not be used for compressed dictionary files which could have
// more compression chunk headers.
//------------------------------------------------------------------------------
unsigned int CompressInterface::getPtrCount(const char* hdrBuf)
{
    return getPtrCount(hdrBuf + HDR_BUF_LEN, HDR_BUF_LEN);
}

//------------------------------------------------------------------------------
// Store list of compression pointers into the specified header.
//------------------------------------------------------------------------------
void CompressInterface::storePtrs(const std::vector<uint64_t>& ptrs,
                                  void* ptrBuf, int ptrSectionSize)
{
    memset((ptrBuf), 0, ptrSectionSize); // reset the pointer section to 0
    uint64_t* hdrPtrs = reinterpret_cast<uint64_t*>(ptrBuf);

    for (unsigned i = 0; i < ptrs.size(); i++)
    {
        hdrPtrs[i] = ptrs[i];
    }
}

//------------------------------------------------------------------------------
// Wrapper of above method for backward compatibility
//------------------------------------------------------------------------------
void CompressInterface::storePtrs(const std::vector<uint64_t>& ptrs, void* ptrBuf)
{
    storePtrs(ptrs, reinterpret_cast<char*>(ptrBuf) + HDR_BUF_LEN, HDR_BUF_LEN);
}

//------------------------------------------------------------------------------
// Initialize the header blocks to be written at the start of a column file.
//------------------------------------------------------------------------------
void CompressInterface::initHdr(void* hdrBuf, int compressionType)
{
    memset(hdrBuf, 0, HDR_BUF_LEN * 2);
    initCompressedDBFileHeader(hdrBuf, compressionType, HDR_BUF_LEN * 2);
}

//------------------------------------------------------------------------------
// Initialize the header blocks to be written at the start of a dictionary file.
//------------------------------------------------------------------------------
void CompressInterface::initHdr(void* hdrBuf, void* ptrBuf,
                                int compressionType, int hdrSize)
{
    memset(hdrBuf, 0, HDR_BUF_LEN);
    memset(ptrBuf, 0, hdrSize - HDR_BUF_LEN);
    initCompressedDBFileHeader(hdrBuf, compressionType, hdrSize);
}

//------------------------------------------------------------------------------
// Initialize the header blocks to be written at the start of a column file.
//------------------------------------------------------------------------------
void CompressInterface::initHdr(
    void* hdrBuf, uint32_t columnWidth,
    execplan::CalpontSystemCatalog::ColDataType columnType,
    int compressionType)
{
    memset(hdrBuf, 0, HDR_BUF_LEN * 2);
    initCompressedDBFileHeader(hdrBuf, columnWidth, columnType,
                               compressionType, HDR_BUF_LEN * 2);
}

//------------------------------------------------------------------------------
// Set the file's block count
//------------------------------------------------------------------------------
void CompressInterface::setBlockCount(void* hdrBuf, uint64_t count)
{
    reinterpret_cast<CompressedDBFileHeader*>(hdrBuf)->fBlockCount = count;
}

//------------------------------------------------------------------------------
// Get the file's block count
//------------------------------------------------------------------------------
uint64_t CompressInterface::getBlockCount(const void* hdrBuf)
{
    return (reinterpret_cast<const CompressedDBFileHeader*>(hdrBuf)->fBlockCount);
}

//------------------------------------------------------------------------------
// Get the file's compression type
//------------------------------------------------------------------------------
uint64_t CompressInterface::getCompressionType(const void* hdrBuf)
{
    return (reinterpret_cast<const CompressedDBFileHeader*>(hdrBuf)
                ->fCompressionType);
}

//------------------------------------------------------------------------------
// Set the overall header size
//------------------------------------------------------------------------------
void CompressInterface::setHdrSize(void* hdrBuf, uint64_t size)
{
    reinterpret_cast<CompressedDBFileHeader*>(hdrBuf)->fHeaderSize = size;
}

//------------------------------------------------------------------------------
// Get the overall header size
//------------------------------------------------------------------------------
uint64_t CompressInterface::getHdrSize(const void* hdrBuf)
{
    return (reinterpret_cast<const CompressedDBFileHeader*>(hdrBuf)->fHeaderSize);
}

//------------------------------------------------------------------------------
// Calculates the chunk and block offset within the chunk for the specified
// block number.
//------------------------------------------------------------------------------
void CompressInterface::locateBlock(unsigned int block,
                                    unsigned int& chunkIndex,
                                    unsigned int& blockOffsetWithinChunk) const
{
    const uint64_t BUFLEN  = UNCOMPRESSED_INBUF_LEN;

    uint64_t byteOffset    = (uint64_t)block * BLOCK_SIZE;
    uint64_t chunk         = byteOffset / BUFLEN;
    uint64_t blockInChunk  = (byteOffset % BUFLEN) / BLOCK_SIZE;

    chunkIndex             = chunk;
    blockOffsetWithinChunk = blockInChunk;
}

//------------------------------------------------------------------------------
// Round up the size of the buffer to the applicable compressed size increment,
// also expand to allow for user requested padding.  Lastly, initialize padding
// bytes to 0.
//------------------------------------------------------------------------------
int CompressInterface::padCompressedChunks(unsigned char* buf, size_t& len,
                                           unsigned int maxLen) const
{
    int nPaddingBytes = 0;
    int nRem = len % COMPRESSED_CHUNK_INCREMENT_SIZE;

    if (nRem != 0)
    {
        nPaddingBytes = COMPRESSED_CHUNK_INCREMENT_SIZE - nRem;
    }

    nPaddingBytes = nPaddingBytes + fNumUserPaddingBytes;

    if (nPaddingBytes > 0)
    {
        if ((len + nPaddingBytes) > maxLen)
            return -1;

        memset(buf + len, 0, nPaddingBytes);
        len = len + nPaddingBytes;
    }

    return 0;
}

CompressInterfaceSnappy::CompressInterfaceSnappy(uint32_t numUserPaddingBytes)
    : CompressInterface(numUserPaddingBytes)
{
}

int32_t CompressInterfaceSnappy::compress(const char* in, size_t inLen,
                                          char* out, size_t* outLen) const
{
    snappy::RawCompress(in, inLen, out, outLen);
    return ERR_OK;
}

int32_t CompressInterfaceSnappy::uncompress(const char* in, size_t inLen,
                                            char* out, size_t* outLen) const
{
    size_t realOutLen = 0;
    auto rc = snappy::GetUncompressedLength(in, inLen, &realOutLen);

    if (!rc || realOutLen > *outLen)
    {
        cerr << "snappy::GetUncompressedLength failed. InLen: " << inLen
             << ", outLen: " << *outLen << ", realOutLen: " << realOutLen
             << endl;
        return ERR_DECOMPRESS;
    }

    rc = snappy::RawUncompress(in, inLen, out);

    if (!rc)
    {
        cerr << "snappy::RawUnompress failed. InLen: " << inLen
             << ", outLen: " << *outLen << endl;
        return ERR_DECOMPRESS;
    }

    return ERR_OK;
}

size_t CompressInterfaceSnappy::maxCompressedSize(size_t uncompSize) const
{
    return (snappy::MaxCompressedLength(uncompSize) + HEADER_SIZE);
}

bool CompressInterfaceSnappy::getUncompressedSize(char* in, size_t inLen,
                                                  size_t* outLen) const
{
    return snappy::GetUncompressedLength(in, inLen, outLen);
}

uint8_t CompressInterfaceSnappy::getChunkMagicNumber() const
{
    return CHUNK_MAGIC_SNAPPY;
}

CompressInterface* getCompressInterfaceByType(uint32_t compressionType,
                                              uint32_t numUserPaddingBytes)
{
    idbassert(compressionType > 0 && compressionType <= 2);
    // Only one compression type is available currently.
    return new CompressInterfaceSnappy(numUserPaddingBytes);
}

#endif

} // namespace compress
// vim:ts=4 sw=4:

