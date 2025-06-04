#pragma once

#include <vector>
#include <cmath>

#include "xxhash.h"

// Only for debug
#include <fstream>

namespace bloom_funcs
{
  template<typename T>
  static inline void lg1(T data)
  {
    std::ofstream log("/tmp/bc_udf.log", std::ios::app);
    log << data << "\n";
  }
  
inline constexpr uint64_t seed1 = 77557187;
inline constexpr uint64_t seed2 = 8012791231;
inline constexpr uint64_t blockSize = 8;

using BlockType = uint8_t;
using BloomFilter = std::vector<BlockType>;

inline uint64_t getHashFuncCount(size_t maxElemCount, size_t bfBitCount);
inline uint64_t getBloomFilterBitCount(size_t maxElemCount, double falsePosRate);

// Data for Stub Connector
struct BloomData
{
  BloomData() = default;

  BloomData(size_t maxElemCount, double falsePosRate) 
  {
    // lg1("max elem count: ");
    // lg1(maxElemCount);
    // lg1("false pos rate: ");
    // lg1(falsePosRate);

    auto bfBitCount = getBloomFilterBitCount(maxElemCount, falsePosRate/100);
    fHashFuncCount = getHashFuncCount(maxElemCount, bfBitCount);
    bloomFilter.resize((bfBitCount + blockSize - 1) / blockSize);
    // lg1("BloomData constructor. Bloom filter size: ");
    // lg1(bloomFilter.size());
    // lg1("hash func count: ");
    // lg1(fHashFuncCount);
  }

  BloomFilter bloomFilter = BloomFilter(100, 0);
  size_t fHashFuncCount;
};

inline uint64_t getHashFuncCount(size_t maxElemCount, size_t bfBitCount)
{
  constexpr double ln2 = std::log(2.0);
  return static_cast<uint64_t>(std::round((bfBitCount / (static_cast<double>(maxElemCount))) * ln2));
}

inline uint64_t getBloomFilterBitCount(size_t maxElemCount, double falsePosRate)
{
  constexpr double ln2Sqr = std::log(2.0) * std::log(2.0);
  return static_cast<uint64_t>(std::round(-(maxElemCount * std::log(falsePosRate)) / ln2Sqr));
}

template<typename T, typename Data>
inline void addValueToBloomFilter(const T& val, Data& data)
{
  uint64_t blockIdxHash = XXH3_64bits(reinterpret_cast<const void*>(&val), sizeof(val)) % data.bloomFilter.size();
  
  uint64_t valHash1 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed1);
  uint64_t valHash2 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed2);

  auto& block = data.bloomFilter[blockIdxHash];
  
  for (size_t i = 0; i < data.fHashFuncCount; ++i) {
    size_t bitIdx = (valHash1 + i * valHash2);
    bitIdx &= (blockSize - 1);
    block |= (static_cast<BlockType>(1) << (bitIdx));
  }
}

template<typename T>
inline int64_t bloomFilterContains(const T& val, const BloomFilter& bloomFilter, const size_t hashFuncCount)
{
    if (bloomFilter.empty()) return 0;

    uint64_t blockIdxHash = XXH3_64bits(reinterpret_cast<const void*>(&val), sizeof(val)) % bloomFilter.size();

    uint64_t valHash1 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed1);
    uint64_t valHash2 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed2);

    const auto& block = bloomFilter[blockIdxHash];

    for (size_t i = 0; i < hashFuncCount; ++i) {
        size_t bitIdx = (valHash1 + i * valHash2);
        bitIdx &= (blockSize - 1);
        if ((block & (static_cast<BlockType>(1) << bitIdx)) == 0) {
            return 0;
        }
    }

    return 1;
}

}