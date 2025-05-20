#pragma once

#include "xxhash.h"

namespace bloom_funcs
{

inline const uint64_t seed1 = 77557187;
inline const uint64_t seed2 = 8012791231;

template<typename T, typename Data>
inline void addValueToBloomFilter(const T& val, Data& data)
{
  uint64_t blockIdxHash = XXH3_64bits(reinterpret_cast<const void*>(&val), sizeof(val)) % data.bloomFilter.size();
  
  uint64_t valHash1 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed1);
  uint64_t valHash2 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed2);

  auto& block = data.bloomFilter[blockIdxHash];
  
  for (size_t i = 0; i < data.hashFuncCount; ++i) {
    size_t bitIdx = (valHash1 + i * valHash2) % 8;
    block |= (1 << (bitIdx));
  }
}




}