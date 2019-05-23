
#include <gtest/gtest.h>
#include <boost/atomic/atomic.hpp>
#include <boost/thread/thread.hpp>
#include <vector>
#include <map>
#include "../../utils/common/shared_pool_allocator.h"
#include "../../utils/common/simpleallocator.h"
#include "../../utils/common/stlpoolallocator.h"
#include "../../utils/common/poolallocator.h"
#include "../../utils/common/poolallocator.cpp"


namespace{

  boost::atomic<bool> STLPoolMTSmallObjectsQuit;

  void STLPoolMTSmallObjectsThread() {
    typedef std::vector<size_t, utils::STLPoolAllocator<size_t>> vector_type;
    utils::STLPoolAllocator<size_t> oAllocator(0);
    vector_type oVector(oAllocator);
    while (!STLPoolMTSmallObjectsQuit.load()) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

  }

  TEST(STLPoolAllocatorTest, MTSmallObjects) {
    typedef std::vector<size_t, utils::STLPoolAllocator<size_t>> vector_type;
    STLPoolMTSmallObjectsQuit.store(false);
    std::vector<boost::thread> oThreads;
    for (int i = 0; i < 7; ++i) {
      oThreads.push_back(boost::thread(STLPoolMTSmallObjectsThread));
    }
    
    utils::STLPoolAllocator<size_t> oAllocator(0);
    vector_type oVector(oAllocator);
    for (size_t i = 0; i < 10000000; ++i) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

    STLPoolMTSmallObjectsQuit.store(true);
    for (auto& oThread : oThreads) {
      oThread.join();
    }
  }


  boost::atomic<bool> SimpleMTSmallObjectsQuit;


  void SimpleMTSmallObjectsThread() {
    typedef std::vector<size_t, utils::SimpleAllocator<size_t>> vector_type;
    boost::shared_ptr<utils::SimplePool> oPool(new utils::SimplePool);
    utils::SimpleAllocator<size_t> oAllocator(oPool);
    vector_type oVector(oAllocator);
    while (!SimpleMTSmallObjectsQuit.load()) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

  }

  TEST(SimpleAllocatorTest, MTSmallObjects) {
    typedef std::vector<size_t, utils::SimpleAllocator<size_t>> vector_type;
    SimpleMTSmallObjectsQuit.store(false);
    std::vector<boost::thread> oThreads;
    for (int i = 0; i < 7; ++i) {
      oThreads.push_back(boost::thread(SimpleMTSmallObjectsThread));
    }
    
    boost::shared_ptr<utils::SimplePool> oPool(new utils::SimplePool);
    utils::SimpleAllocator<size_t> oAllocator(oPool);
    vector_type oVector(oAllocator);
    for (size_t i = 0; i < 10000000; ++i) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

    SimpleMTSmallObjectsQuit.store(true);
    for (auto& oThread : oThreads) {
      oThread.join();
    }
  }


  boost::atomic<bool> shared_pool_MTSmallObjectsQuit;


  void MTSmallObjectsThread2() {
    typedef std::vector<size_t, utils::shared_pool_allocator<size_t>> vector_type;
    vector_type oVector;
    while (!shared_pool_MTSmallObjectsQuit.load()) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

  }

  TEST(shared_pool_allocator_test, MTSmallObjects) {
    typedef std::vector<size_t, utils::shared_pool_allocator<size_t>> vector_type;
    shared_pool_MTSmallObjectsQuit.store(false);
    std::vector<boost::thread> oThreads;
    for (int i = 0; i < 7; ++i) {
      oThreads.push_back(boost::thread(MTSmallObjectsThread2));
    }
        vector_type oVector;
    for (size_t i = 0; i < 1000000; ++i) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

    shared_pool_MTSmallObjectsQuit.store(true);
    for (auto& oThread : oThreads) {
      oThread.join();
    }
  }


  TEST(STLPoolAllocatorTest, STSmallObjects) {
    typedef std::vector<size_t, utils::STLPoolAllocator<size_t>> vector_type;
    utils::STLPoolAllocator<size_t> oAllocator(0);
    vector_type oVector(oAllocator);
    for (size_t i = 0; i < 100000000; ++i) {
      oVector.push_back(i);
      oVector.pop_back();
    }

  }

	TEST(SimpleAllocatorTest, STSmallObjects) {
    typedef std::vector<size_t, utils::SimpleAllocator<size_t>> vector_type;
    boost::shared_ptr<utils::SimplePool> oPool(new utils::SimplePool);
    utils::SimpleAllocator<size_t> oAllocator(oPool);
    vector_type oVector(oAllocator);
		for (size_t i = 0; i < 100000000; ++i) {
			oVector.push_back(i);
  		oVector.pop_back();
    }

  }

  TEST(shared_pool_allocator_test, STSmallObjects) {
    typedef std::vector<size_t, utils::shared_pool_allocator<size_t>> vector_type;
    vector_type oVector;
    for (size_t i = 0; i < 100000000; ++i) {
      oVector.push_back(i);
      oVector.pop_back();
    }
  }


  TEST(STLPoolAllocator, container_construction) {
    typedef std::vector<size_t, utils::STLPoolAllocator<size_t>> vector_type;
    for (size_t i = 0; i < 10000000; ++i) {
      utils::STLPoolAllocator<size_t> oAllocator(0);
	    vector_type oVector(oAllocator);
    }

  }

  TEST(SimpleAllocatorTest, container_construction) {
      typedef std::vector<size_t, utils::SimpleAllocator<size_t>> vector_type;
      for (size_t i = 0; i < 10000000; ++i) {
        boost::shared_ptr<utils::SimplePool> oPool(new utils::SimplePool);
        utils::SimpleAllocator<size_t> oAllocator(oPool);
        vector_type oVector(oAllocator);
      }

    }


  TEST(shared_pool_allocator_test, container_construction) {
    typedef std::vector<size_t, utils::shared_pool_allocator<size_t>> vector_type;
    for (size_t i = 0; i < 10000000; ++i) {
      vector_type oVector;
    }
  }


  
}