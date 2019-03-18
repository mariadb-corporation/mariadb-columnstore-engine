
add_catch2_test(tbb_memory_pool_test.cpp)

target_link_libraries(tbb_memory_pool_test PRIVATE common catch2 Threads::Threads tbb tbbmalloc)