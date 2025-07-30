#include <vector>
#include <atomic>
#include <cmath>

namespace joblist
{

class BlockedBloomFilter
{
    public:
        // TODO
        BlockedBloomFilter() {}

        void insert(uint64_t hash);
        bool probe(uint64_t hash) const;

    private:
        // Member variables
        static constexpr uint32_t SALTS[HASH_FUNC_COUNT] = 
        {
            0x47b6137b,
            0x44974d91,
            0x8824ad5b,
            0xa2b7289d,
            0x705495c7,
            0x2df1424b,
            0x9efc4947,
            0x5c6bfb31
        };
        
        // Calculating BF's parameters at compile-time
        static constexpr uint8_t BLOCK_SIZE = 64;
        static constexpr uint8_t HASH_FUNC_COUNT = 8;
        static constexpr uint32_t EXTENT_SIZE = 8'000'000UL;
        static constexpr uint32_t DOUBLE_EXTENT_SIZE = 2*8'000'000UL;
        static constexpr double FALSE_POSITIVE_RATE = 0.01;
        static constexpr double lnFP = 4.605170186; // lnFP <- |ln(FPR)|
        static constexpr double ln2sqr = 0.4804530139; // pow(ln(2), 2)
        static constexpr uint64_t NUMBER_OF_BITS = (EXTENT_SIZE * lnFP) / ln2sqr;
        static constexpr uint64_t BLOOM_FILTER_BLOCK_COUNT = (NUMBER_OF_BITS + BLOCK_SIZE - 1) / BLOCK_SIZE;
        
        // Possible to make it as array, at least the first 8 mln filter
        std::vector<std::atomic<uint64_t>> bloomFilter;

        // Private member functions
        inline uint64_t mix64(uint64_t hash) const;

};




} // namespace joblist