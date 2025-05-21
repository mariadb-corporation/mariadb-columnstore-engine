#pragma once

#include <vector>

#include "xxhash.h"

namespace bloom_funcs
{

inline const uint64_t seed1 = 77557187;
inline const uint64_t seed2 = 8012791231;

// Data for Stub Connector
struct BloomData
{
  BloomData(size_t hashFuncCount) : fHashFuncCount(hashFuncCount)
  {

  }

  std::vector<uint8_t> bloomFilter = std::vector<uint8_t>();;
  size_t fHashFuncCount;
};

template<typename T, typename Data>
inline void addValueToBloomFilter(const T& val, Data& data)
{
  uint64_t blockIdxHash = XXH3_64bits(reinterpret_cast<const void*>(&val), sizeof(val)) % data.bloomFilter.size();
  
  uint64_t valHash1 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed1);
  uint64_t valHash2 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed2);

  auto& block = data.bloomFilter[blockIdxHash];
  
  for (size_t i = 0; i < data.fHashFuncCount; ++i) {
    size_t bitIdx = (valHash1 + i * valHash2) % 8;
    block |= (1 << (bitIdx));
  }
}

template<typename T>
inline bool bloomFilterContains(const T& val, const std::vector<uint8_t>& bloomFilter, const size_t hashFuncCount)
{
    if (bloomFilter.empty()) return false;

    uint64_t blockIdxHash = XXH3_64bits(reinterpret_cast<const void*>(&val), sizeof(val)) % bloomFilter.size();

    uint64_t valHash1 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed1);
    uint64_t valHash2 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed2);

    const auto& block = bloomFilter[blockIdxHash];

    for (size_t i = 0; i < hashFuncCount; ++i) {
        size_t bitIdx = (valHash1 + i * valHash2) % 8;
        if ((block & (1 << bitIdx)) == 0) {
            return false;
        }
    }

    return true;
}




}