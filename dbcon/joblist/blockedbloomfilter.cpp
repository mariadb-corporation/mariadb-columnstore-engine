#include "blockedbloomfilter.h"

namespace joblist
{


void BlockedBloomFilter::insert(uint64_t hash)
{
    uint64_t blockIdx = hash % BLOOM_FILTER_BLOCK_COUNT;
    uint64_t bitmask = 0;
    
    for (const auto& salt : SALTS)
    {
        uint64_t mixed = mix64(hash ^ static_cast<uint64_t>(salt));

        uint64_t bitIdx = mixed % 64;
        bitmask |= (1ULL << bitIdx); 
    }

    bloomFilter[blockIdx].fetch_or(bitmask, std::memory_order_relaxed);
}


bool BlockedBloomFilter::probe(uint64_t hash) const
{
    uint64_t blockIdx = hash % BLOOM_FILTER_BLOCK_COUNT;
    uint64_t block = bloomFilter[blockIdx].load(std::memory_order_relaxed);

    for (const auto& salt : SALTS)
    {
        uint64_t mixed = mix64(hash ^ static_cast<uint64_t>(salt));
        uint64_t bitIdx = mixed % 64;

        if ((block & (1ULL << bitIdx)) == 0)
        {
            return false;
        }
        
    }

    return true;
}

// SplitMix
inline uint64_t BlockedBloomFilter::mix64(uint64_t hash) const
{
    hash ^= hash >> 30;
    hash *= 0xbf58476d1ce4e5b9ULL;
    hash ^= hash >> 27;
    hash *= 0x94d049bb133111ebULL;
    hash ^= hash >> 31;

    return hash;
}



} // namespace joblist

