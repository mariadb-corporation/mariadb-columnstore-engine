
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

  boost::atomic<bool> MTSmallObjectsQuit;

#if 1
  void MTSmallObjectsThread() {
    typedef std::vector<size_t, utils::STLPoolAllocator<size_t>> vector_type;
    utils::STLPoolAllocator<size_t> oAllocator(0);
    vector_type oVector(oAllocator);
    while (!MTSmallObjectsQuit.load()) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

  }

  TEST(SimpleAllocatorTest, MTSmallObjects) {
    typedef std::vector<size_t, utils::STLPoolAllocator<size_t>> vector_type;
    MTSmallObjectsQuit.store(false);
    std::vector<boost::thread> oThreads;
    for (int i = 0; i < 7; ++i) {
      oThreads.push_back(boost::thread(MTSmallObjectsThread));
    }
    
    utils::STLPoolAllocator<size_t> oAllocator(0);
    vector_type oVector(oAllocator);
    for (size_t i = 0; i < 10000000; ++i) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

    MTSmallObjectsQuit.store(true);
    for (auto& oThread : oThreads) {
      oThread.join();
    }
  }

#endif

    
#if 0
  void MTSmallObjectsThread() {
    typedef std::vector<size_t, utils::SimpleAllocator<size_t>> vector_type;
    boost::shared_ptr<utils::SimplePool> oPool(new utils::SimplePool);
    utils::SimpleAllocator<size_t> oAllocator(oPool);
    vector_type oVector(oAllocator);
    while (!MTSmallObjectsQuit.load()) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

  }

  TEST(SimpleAllocatorTest, MTSmallObjects) {
    typedef std::vector<size_t, utils::SimpleAllocator<size_t>> vector_type;
    MTSmallObjectsQuit.store(false);
    std::vector<boost::thread> oThreads;
    for (int i = 0; i < 7; ++i) {
      oThreads.push_back(boost::thread(MTSmallObjectsThread));
    }
    
    boost::shared_ptr<utils::SimplePool> oPool(new utils::SimplePool);
    utils::SimpleAllocator<size_t> oAllocator(oPool);
    vector_type oVector(oAllocator);
    for (size_t i = 0; i < 10000000; ++i) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

    MTSmallObjectsQuit.store(true);
    for (auto& oThread : oThreads) {
      oThread.join();
    }
  }

#endif

    
#if 0
  void MTSmallObjectsThread2() {
    typedef std::vector<size_t, utils::shared_pool_allocator<size_t>> vector_type;
    vector_type oVector;
    while (!MTSmallObjectsQuit.load()) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

  }

  TEST(SimpleAllocatorTest, MTSmallObjects) {
    typedef std::vector<size_t, utils::shared_pool_allocator<size_t>> vector_type;
    MTSmallObjectsQuit.store(false);
    std::vector<boost::thread> oThreads;
    for (int i = 0; i < 7; ++i) {
      oThreads.push_back(boost::thread(MTSmallObjectsThread2));
    }
        vector_type oVector;
    for (size_t i = 0; i < 1000000; ++i) {
      for (int i = 0; i < 50; ++i) oVector.push_back(0);
      for (int i = 0; i < 50; ++i) oVector.pop_back();
    }

    MTSmallObjectsQuit.store(true);
    for (auto& oThread : oThreads) {
      oThread.join();
    }
  }

#endif

#if 0
  TEST(SimpleAllocatorTest, STSmallObjects) {
    typedef std::vector<size_t, utils::STLPoolAllocator<size_t>> vector_type;
    utils::STLPoolAllocator<size_t> oAllocator(0);
    vector_type oVector(oAllocator);
    for (size_t i = 0; i < 100000000; ++i) {
      oVector.push_back(i);
      oVector.pop_back();
    }

  }
#endif

#if 0
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
#endif
#if 0

  TEST(shared_pool_allocator_test, STSmallObjects) {
    typedef std::vector<size_t, utils::shared_pool_allocator<size_t>> vector_type;
    vector_type oVector;
    for (size_t i = 0; i < 100000000; ++i) {
      oVector.push_back(i);
      oVector.pop_back();
    }
  }
#endif

#if 0
  TEST(STLPoolAllocator, container_construction) {
    typedef std::vector<size_t, utils::STLPoolAllocator<size_t>> vector_type;
    utils::STLPoolAllocator<size_t> oAllocator(0);
    for (size_t i = 0; i < 10000000; ++i) {
      vector_type oVector(oAllocator);
    }

  }
#endif

#if 0
  TEST(SimpleAllocatorTest, container_construction) {
      typedef std::vector<size_t, utils::SimpleAllocator<size_t>> vector_type;
	    boost::shared_ptr<utils::SimplePool> oPool(new utils::SimplePool);
      utils::SimpleAllocator<size_t> oAllocator(oPool);
      for (size_t i = 0; i < 10000000; ++i) {
        vector_type oVector(oAllocator);
      }

    }
#endif
#if 0

  TEST(shared_pool_allocator_test, container_construction) {
    typedef std::vector<size_t, utils::shared_pool_allocator<size_t>> vector_type;
    for (size_t i = 0; i < 10000000; ++i) {
      vector_type oVector;
    }
  }
#endif


#if 0
	boost::atomic<bool> QuitFlag;

  typedef std::vector<size_t, utils::SimpleAllocator<size_t>> vector_type;

	void push_and_pop_thread() {
  	boost::shared_ptr<utils::SimplePool> oPool(new utils::SimplePool);
  	utils::SimpleAllocator<size_t> oAllocator(oPool);
    while (!QuitFlag.load()){
      vector_type oVector(oAllocator);
	    for (int i = 0; i < 500; ++i) oVector.push_back(i);
      for (int i = 0; i < 500; ++i) oVector.pop_back();
      boost::this_thread::yield();
    }
	}

	TEST(shared_pool_allocator_test, permits_concurrent_access){
		QuitFlag.store(false);
  	std::vector<boost::thread> oThreads;
    for (int i=0 ; i< 7 ; ++i) {
      oThreads.push_back(boost::thread(push_and_pop_thread));
    }
    
  	boost::shared_ptr<utils::SimplePool> oPool(new utils::SimplePool);
  	utils::SimpleAllocator<size_t> oAllocator(oPool);
  	vector_type oVector(oAllocator);
		for (size_t i=0  ; i<100000000 ; ++i) {
		    oVector.push_back(i);
		}

    QuitFlag.store(true);
  	for (int i = 0; i < 7; ++i) {
    	oThreads[i].join();
  	}
	}
#endif
}