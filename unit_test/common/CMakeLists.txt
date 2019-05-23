
message(STATUS "allocator tests")

add_gtest(shared_pool_allocator_test shared_pool_allocator_test.cpp)
target_link_libraries(shared_pool_allocator_test PRIVATE Boost::system Boost::thread)
