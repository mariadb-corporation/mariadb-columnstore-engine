#include "blockedbloomfilter.h"

namespace joblist
{


void BlockedBloomFilter::insert(uint32_t hash)
{
    uint32_t blockIdx = hash % BLOOM_FILTER_BLOCK_COUNT;
    uint64_t bitmask = 0;
    
    for (const auto& salt : SALTS)
    {
        uint32_t mixed = mix32(hash ^ salt);
        uint8_t bitIdx = mixed % 64;

        bitmask |= (1ULL << bitIdx); 
    }

    bloomFilter[blockIdx].fetch_or(bitmask, std::memory_order_relaxed);
}


bool BlockedBloomFilter::probe(uint32_t hash) const
{
    uint32_t blockIdx = hash % BLOOM_FILTER_BLOCK_COUNT;
    uint64_t block = bloomFilter[blockIdx].load(std::memory_order_relaxed);

    for (const auto& salt : SALTS)
    {
        uint32_t mixed = mix32(hash ^ salt);
        uint8_t bitIdx = mixed % 64;

        if ((block & (1ULL << bitIdx)) == 0)
        {
            return false;
        }
        
    }

    return true;
}

// SplitMix
inline uint32_t BlockedBloomFilter::mix32(uint32_t hash) const
{
    hash ^= hash >> 16;
    hash *= 0x85ebca6b;
    hash ^= hash >> 13;
    hash *= 0xc2b2ae35;
    hash ^= hash >> 16;

    return hash;
}



} // namespace joblist

