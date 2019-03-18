#include <vector>
#include <tbb/tbb_allocator.h>
#define TBB_PREVIEW_MEMORY_POOL 1
#include <tbb/memory_pool.h>
#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>

TEST_CASE("stl allocator remains within memory usage threshold", "[tbb]"){

  using pool_type = tbb::memory_pool<tbb::scalable_allocator<int> > ;
  using allocator_type = tbb::memory_pool_allocator<int>;
  using vector_type = std::vector<int, allocator_type>;


  pool_type oMemoryPool;
  allocator_type oAllocator(oMemoryPool);
  vector_type oVector(oAllocator);
  for (int i=0 ; i<100 ; ++i){
    oVector.push_back(i);
  }
}