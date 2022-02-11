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

/** @file */

#ifndef IDBCOMPRESS_H__
#define IDBCOMPRESS_H__

#include <unistd.h>
#ifdef __linux__
#include <sys/types.h>
#endif
#include <vector>
#include <utility>
#include <unordered_map>

#include "calpontsystemcatalog.h"

#if defined(_MSC_VER) && defined(xxxIDBCOMP_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace compress
{
typedef std::pair<uint64_t, uint64_t> CompChunkPtr;
typedef std::vector<CompChunkPtr> CompChunkPtrList;

class CompressInterface
{
 public:
  static const unsigned int HDR_BUF_LEN = 4096;
  static const unsigned int UNCOMPRESSED_INBUF_LEN = 512 * 1024 * 8;
  static const uint32_t COMPRESSED_CHUNK_INCREMENT_SIZE = 8192;

  // error codes from uncompressBlock()
  static const int ERR_OK = 0;
  static const int ERR_CHECKSUM = -1;
  static const int ERR_DECOMPRESS = -2;
  static const int ERR_BADINPUT = -3;
  static const int ERR_BADOUTSIZE = -4;
  static const int ERR_COMPRESS = -5;

  /**
   * When CompressInterface object is being used to compress a chunk, this
   * construct can be used to specify the padding added by padCompressedChunks
   */
  EXPORT explicit CompressInterface(unsigned int numUserPaddingBytes = 0);

  /**
   * dtor
   */
  EXPORT virtual ~CompressInterface() = default;

  /**
   * see if the algo is available in this lib
   */
  EXPORT static bool isCompressionAvail(int compressionType = 0);

  /**
   * Returns the maximum compressed size from all available compression
   * types.
   */
  EXPORT static size_t getMaxCompressedSizeGeneric(size_t inLen);

  /**
   * Compresses specified "in" buffer of length "inLen" bytes.
   * Compressed data and size are returned in "out" and "outLen".
   * "out" should be sized using maxCompressedSize() to allow for incompressible data.
   * Returns 0 if success.
   */

  EXPORT int compressBlock(const char* in, const size_t inLen, unsigned char* out, size_t& outLen) const;

  /**
   * outLen must be initialized with the size of the out buffer before calling uncompressBlock.
   * On return, outLen will have the number of bytes used in out.
   */
  EXPORT int uncompressBlock(const char* in, const size_t inLen, unsigned char* out, size_t& outLen) const;

  /**
   * This fcn wraps whatever compression algorithm we're using at the time, and
   * is not specific to blocks on disk.
   */
  EXPORT virtual int compress(const char* in, size_t inLen, char* out, size_t* outLen) const = 0;

  /**
   * This fcn wraps whatever compression algorithm we're using at the time, and
   * is not specific to blocks on disk.  The caller needs to make sure out is big
   * enough to contain the output by using getUncompressedSize().
   */
  EXPORT virtual int uncompress(const char* in, size_t inLen, char* out, size_t* outLen) const = 0;

  /**
   * Initialize header buffer at start of compressed db file.
   *
   * @warning hdrBuf must be at least HDR_BUF_LEN bytes
   * @warning ptrBuf must be at least (hdrSize-HDR_BUF_LEN) bytes
   */
  EXPORT static void initHdr(void* hdrBuf, void* ptrBuf, uint32_t columnWidht,
                             execplan::CalpontSystemCatalog::ColDataType columnType, int compressionType,
                             int hdrSize);
  /**
   * Initialize header buffer at start of compressed db file.
   *
   * @warning hdrBuf must be at least HDR_BUF_LEN*2 bytes
   */
  EXPORT static void initHdr(void* hdrBuf, uint32_t columnWidth,
                             execplan::CalpontSystemCatalog::ColDataType columnType, int compressionType);

  /**
   * Verify the passed in buffer contains a compressed db file header.
   */
  EXPORT static int verifyHdr(const void* hdrBuf);

  /**
   * Extracts list of compression pointers from the specified ptr buffer.
   * ptrBuf points to the pointer section taken from the headers.
   * chunkPtrs is a vector of offset, size pairs for the compressed chunks.
   * Returns 0 if success.
   */
  EXPORT static int getPtrList(const char* ptrBuf, const int ptrBufSize, CompChunkPtrList& chunkPtrs);

  /**
   * Extracts list of compression pointers from the specified header.
   * hdrBuf points to start of 2 buffer headers from compressed db file.
   * Overloaded for backward compatibility. For none dictionary columns.
   * Note: the pointer passed in is the beginning of the header,
   *       not the pointer section as above.
   */
  EXPORT static int getPtrList(const char* hdrBuf, CompChunkPtrList& chunkPtrs);

  /**
   * Return the number of chunk pointers contained in the specified ptr buffer.
   * ptrBuf points to the pointer section taken from the headers.
   */
  EXPORT static unsigned int getPtrCount(const char* ptrBuf, const int ptrBufSize);

  /**
   * Return the number of chunk pointers contained in the specified header.
   * hdrBuf points to start of 2 buffer headers from compressed db file.
   * For non-dictionary columns.
   */
  EXPORT static unsigned int getPtrCount(const char* hdrBuf);

  /**
   * Store vector of pointers into the specified buffer header's pointer section.
   */
  EXPORT static void storePtrs(const std::vector<uint64_t>& ptrs, void* hdrBuf, int ptrSectionSize);

  /**
   * Store vector of pointers into the specified buffer header.
   * Overloaded for backward compatibility. For none dictionary columns.
   * Note: the pointer passed in is the beginning of the header,
   *       not the pointer section as above.
   */
  EXPORT static void storePtrs(const std::vector<uint64_t>& ptrs, void* hdrBuf);

  /**
   * Calculates the chunk, and the block offset within the chunk, for the
   * specified block number.
   */
  EXPORT void locateBlock(unsigned int block, unsigned int& chunkIndex,
                          unsigned int& blockOffsetWithinChunk) const;

  /**
   * Pads the specified compressed chunk to the nearest compressed chunk
   * increment, by padding buf with 0's, and updating len accordingly.
   * maxLen is the maximum size for buf.  nonzero return code means the
   * result output buffer length is > than maxLen.
   */
  EXPORT int padCompressedChunks(unsigned char* buf, size_t& len, unsigned int maxLen) const;

  /*
   * Mutator methods for the block count in the file
   */
  /**
   * getVersionNumber
   */
  EXPORT static uint64_t getVersionNumber(const void* hdrBuf);

  /**
   * setBlockCount
   */
  EXPORT static void setBlockCount(void* hdrBuf, uint64_t count);

  /**
   * getBlockCount
   */
  EXPORT static uint64_t getBlockCount(const void* hdrBuf);

  /**
   * getCompressionType
   */
  EXPORT static uint64_t getCompressionType(const void* hdrBuf);

  /*
   * Mutator methods for the overall header size
   */
  /**
   * setHdrSize
   */
  EXPORT static void setHdrSize(void* hdrBuf, uint64_t size);

  /**
   * getHdrSize
   */
  EXPORT static uint64_t getHdrSize(const void* hdrBuf);

  /**
   * getColumnType
   */
  EXPORT static execplan::CalpontSystemCatalog::ColDataType getColDataType(const void* hdrBuf);

  /**
   * getColumnWidth
   */
  EXPORT static uint64_t getColumnWidth(const void* hdrBuf);

  /**
   * getLBIDByIndex
   */
  EXPORT static uint64_t getLBIDByIndex(const void* hdrBuf, uint64_t index);

  /**
   * setLBIDByIndex
   */
  EXPORT static void setLBIDByIndex(void* hdrBuf, uint64_t lbid, uint64_t index);

  /**
   * getLBIDCount
   */
  EXPORT static uint64_t getLBIDCount(void* hdrBuf);

  /**
   * Mutator methods for the user padding bytes
   */
  /**
   * set numUserPaddingBytes
   */
  EXPORT void numUserPaddingBytes(uint64_t num)
  {
    fNumUserPaddingBytes = num;
  }

  /**
   * get numUserPaddingBytes
   */
  EXPORT uint64_t numUserPaddingBytes() const
  {
    return fNumUserPaddingBytes;
  }

  /**
   * Given an input, uncompressed block, what's the maximum possible output,
   * compressed size?
   */
  EXPORT virtual size_t maxCompressedSize(size_t uncompSize) const = 0;

  /**
   * Given a compressed block, returns the uncompressed size in outLen.
   * Returns false on error, true on success.
   */
  EXPORT virtual bool getUncompressedSize(char* in, size_t inLen, size_t* outLen) const = 0;

 protected:
  virtual uint8_t getChunkMagicNumber() const = 0;

 private:
  // defaults okay
  // CompressInterface(const CompressInterface& rhs);
  // CompressInterface& operator=(const CompressInterface& rhs);

  unsigned int fNumUserPaddingBytes;  // Num bytes to pad compressed chunks
};

class CompressInterfaceSnappy : public CompressInterface
{
 public:
  EXPORT CompressInterfaceSnappy(uint32_t numUserPaddingBytes = 0);
  EXPORT ~CompressInterfaceSnappy() = default;
  /**
   * Compress the given block using snappy compression API.
   */
  EXPORT int32_t compress(const char* in, size_t inLen, char* out, size_t* outLen) const override;
  /**
   * Uncompress the given block using snappy compression API.
   */
  EXPORT int32_t uncompress(const char* in, size_t inLen, char* out, size_t* outLen) const override;
  /**
   * Get max compressed size for the given `uncompSize` value using snappy
   * compression API.
   */
  EXPORT size_t maxCompressedSize(size_t uncompSize) const override;

  /**
   * Get uncompressed size for the given block using snappy
   * compression API.
   */
  EXPORT
  bool getUncompressedSize(char* in, size_t inLen, size_t* outLen) const override;

 protected:
  uint8_t getChunkMagicNumber() const override;

 private:
  const uint8_t CHUNK_MAGIC_SNAPPY = 0xfd;
};

class CompressInterfaceLZ4 : public CompressInterface
{
 public:
  EXPORT CompressInterfaceLZ4(uint32_t numUserPaddingBytes = 0);
  EXPORT ~CompressInterfaceLZ4() = default;
  /**
   * Compress the given block using LZ4 compression API.
   */
  EXPORT int32_t compress(const char* in, size_t inLen, char* out, size_t* outLen) const override;
  /**
   * Uncompress the given block using LZ4 compression API.
   */
  EXPORT int32_t uncompress(const char* in, size_t inLen, char* out, size_t* outLen) const override;
  /**
   * Get max compressed size for the given `uncompSize` value using LZ4
   * compression API.
   */
  EXPORT size_t maxCompressedSize(size_t uncompSize) const override;

  /**
   * Get uncompressed size for the given block using LZ4
   * compression API.
   */
  EXPORT
  bool getUncompressedSize(char* in, size_t inLen, size_t* outLen) const override;

 protected:
  uint8_t getChunkMagicNumber() const override;

 private:
  const uint8_t CHUNK_MAGIC_LZ4 = 0xfc;
};

using CompressorPool = std::unordered_map<uint32_t, std::shared_ptr<CompressInterface>>;

/**
 *  Returns a pointer to the appropriate compression interface based on
 * `compressionType`. `compressionType` must be greater than 0.
 *  Note: caller is responsible for memory deallocation.
 */
EXPORT CompressInterface* getCompressInterfaceByType(uint32_t compressionType,
                                                     uint32_t numUserPaddingBytes = 0);

/**
 *  Returns a pointer to the appropriate compression interface based on
 * `compressionName`.
 *  Note: caller is responsible for memory deallocation.
 */
EXPORT CompressInterface* getCompressInterfaceByName(const std::string& compressionName,
                                                     uint32_t numUserPaddingBytes = 0);

/**
 *  Initializes a given `unordered_map` with all available compression
 *  interfaces.
 */
EXPORT void initializeCompressorPool(CompressorPool& compressorPool, uint32_t numUserPaddingBytes = 0);

/**
 *  Returns a `shared_ptr` to the appropriate compression interface.
 */
EXPORT std::shared_ptr<CompressInterface> getCompressorByType(CompressorPool& compressorPool,
                                                              uint32_t compressionType);

#ifdef SKIP_IDB_COMPRESSION
inline CompressInterface::CompressInterface(unsigned int /*numUserPaddingBytes*/)
{
}
inline bool CompressInterface::isCompressionAvail(int c)
{
  return (c == 0);
}
inline int CompressInterface::compressBlock(const char*, const size_t, unsigned char*, size_t&) const
{
  return -1;
}
inline int CompressInterface::uncompressBlock(const char* in, const size_t inLen, unsigned char* out,
                                              size_t& outLen) const
{
  return -1;
}
inline void initHdr(void*, uint32_t, execplan::CalpontSystemCatalog::ColDataType, int)
{
}
inline int CompressInterface::verifyHdr(const void*)
{
  return -1;
}
inline void CompressInterface::initHdr(void*, void*, uint32_t, execplan::CalpontSystemCatalog::ColDataType,
                                       int, int)
{
}
inline void CompressInterface::initHdr(void*, uint32_t, execplan::CalpontSystemCatalog::ColDataType,
                                       int) const
{
}
inline int CompressInterface::getPtrList(const char*, const int, CompChunkPtrList&)
{
  return -1;
}
inline unsigned int CompressInterface::getPtrCount(const char*, const int)
{
  return 0;
}
inline unsigned int CompressInterface::getPtrCount(const char*)
{
  return 0;
}
inline void CompressInterface::storePtrs(const std::vector<uint64_t>&, void*, int)
{
}
inline void CompressInterface::storePtrs(const std::vector<uint64_t>&, void*)
{
}
inline void CompressInterface::locateBlock(unsigned int block, unsigned int& chunkIndex,
                                           unsigned int& blockOffsetWithinChunk) const
{
}
inline int CompressInterface::padCompressedChunks(unsigned char* buf, unsigned int& len,
                                                  unsigned int maxLen) const
{
  return -1;
}
inline uint64_t CompressInterface::getVersionNumber(const void* hdrBuf)
{
  return 0;
}
inline void CompressInterface::setBlockCount(void* hdrBuf, uint64_t count)
{
}
inline uint64_t CompressInterface::getBlockCount(const void* hdrBuf)
{
  return 0;
}
inline uint64_t CompressInterface::getCompressionType(const void* hdrBuf)
{
  return 0;
}
inline execplan::CalpontSystemCatalog::ColDataType CompressInterface::getColDataType(const void* hdrBuf)
{
  return execplan::CalpontSystemCatalog::ColDataType::UNDEFINED;
}
inline uint64_t CompressInterface::getColumnWidth(const void* hdrBuf) const
{
  return 0;
}
inline uint64_t getLBID0(const void* hdrBuf)
{
  return 0;
}
void setLBID0(void* hdrBuf, uint64_t lbid)
{
}
inline uint64_t getLBID1(const void* hdrBuf)
{
  return 0;
}
void setLBID1(void* hdrBuf, uint64_t lbid)
{
}
inline void CompressInterface::setHdrSize(void*, uint64_t)
{
}
inline uint64_t CompressInterface::getHdrSize(const void*)
{
  return 0;
}
CompressInterfaceSnappy::CompressInterfaceSnappy(uint32_t numUserPaddingBytes)
 : CompressInterface(numUserPaddingBytes)
{
}
inline uint64_t IDBCompressInterface::getColumnWidth(const void* hdrBuf) const
{
  return 0;
}
inline uint64_t IDBCompressInterface::maxCompressedSize(uint64_t uncompSize)
{
  return uncompSize;
}
inline uint64_t IDBCompressInterface::getLBIDByIndex(const void* hdrBuf, uint32_t index) const
{
  return 0;
}
void IDBCompressInterface::setLBIDByIndex(void* hdrBuf, uint64_t lbid, uint32_t index) const
{
}
void IDBCompressInterface::getLBIDCount(void* hdrBuf) const
{
}
inline bool IDBCompressInterface::getUncompressedSize(char* in, size_t inLen, size_t* outLen)
{
  return false;
}
uint8_t getChunkMagicNumber() const
{
  return 0;
}
CompressInterface* getCompressInterfaceByType(uint32_t compressionType, uint32_t numUserPaddingBytes)
{
  return nullptr;
}
#endif
}  // namespace compress

#undef EXPORT

#endif
