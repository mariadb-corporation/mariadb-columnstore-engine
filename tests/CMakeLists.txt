include_directories( ${ENGINE_COMMON_INCLUDES} ${ENGINE_BLOCKCACHE_INCLUDE} ${ENGINE_PRIMPROC_INCLUDE})

cmake_policy(SET CMP0054 NEW)
if (WITH_UNITTESTS)
    enable_testing()
    include(GoogleTest)
    find_package(GTest REQUIRED)

    #GoogleTest tests
    add_executable(rowgroup_tests rowgroup-tests.cpp)
    target_link_libraries(rowgroup_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS})
    gtest_add_tests(TARGET rowgroup_tests TEST_PREFIX columnstore:)

    add_executable(arithmeticoperator_tests arithmeticoperator-tests.cpp)
    target_link_libraries(arithmeticoperator_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS})
    gtest_add_tests(TARGET arithmeticoperator_tests TEST_PREFIX columnstore:)

    add_executable(mcs_decimal_tests mcs_decimal-tests.cpp)
    target_link_libraries(mcs_decimal_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS})
    gtest_add_tests(TARGET mcs_decimal_tests TEST_PREFIX columnstore:)

    add_executable(dataconvert_tests dataconvert-tests.cpp)
    target_link_libraries(dataconvert_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS})
    gtest_add_tests(TARGET dataconvert_tests TEST_PREFIX columnstore:)

    add_executable(rebuild_em_tests rebuild-em-tests.cpp)
    target_link_libraries(rebuild_em_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS})
    gtest_add_tests(TARGET rebuild_em_tests TEST_PREFIX columnstore:)

    add_executable(compression_tests compression-tests.cpp)
    target_link_libraries(compression_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS})
    gtest_add_tests(TARGET compression_tests TEST_PREFIX columnstore:)

    add_executable(column_scan_filter_tests primitives_column_scan_and_filter.cpp)
    target_link_libraries(column_scan_filter_tests ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${GTEST_LIBRARIES} processor dbbc)
    gtest_add_tests(TARGET column_scan_filter_tests TEST_PREFIX columnstore:)

    add_executable(simd_processors simd_processors.cpp)
    target_link_libraries(simd_processors ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${GTEST_LIBRARIES} processor dbbc)
    gtest_add_tests(TARGET simd_processors TEST_PREFIX columnstore:)

    # Comment this out b/c FairThreadPoolRemove segfaults in containers. Moreover this code isn't
    # in production yet.
    # add_executable(fair_threadpool_test fair_threadpool.cpp)
    # target_link_libraries(fair_threadpool_test ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${GTEST_LIBRARIES} processor dbbc)
    # gtest_add_tests(TARGET fair_threadpool_test TEST_PREFIX columnstore:)

    # CPPUNIT TESTS
    add_executable(we_shared_components_tests shared_components_tests.cpp)
    add_dependencies(we_shared_components_tests loggingcpp)
    target_link_libraries(we_shared_components_tests ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} cppunit)
    #Fix Test and uncomment
    #add_test(NAME columnstore:we_shared_components_tests, COMMAND we_shared_components_tests)

    add_executable(comparators_tests comparators-tests.cpp)
    target_link_libraries(comparators_tests ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
    add_test(NAME columnstore:comparators_tests, COMMAND comparators_tests)

    # standalone EM routines test
    # add_executable(brm_em_standalone brm-em-standalone.cpp)
    # target_link_libraries(brm_em_standalone ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
    # install(TARGETS brm_em_standalone DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

endif()

# Saving this as the example of the microbench
#if (WITH_MICROBENCHMARKS AND (NOT CMAKE_BUILD_TYPE STREQUAL "debug"))
#    find_package(benchmark REQUIRED)
#    add_executable(primitives_scan_bench primitives_scan_bench.cpp)
#    target_include_directories(primitives_scan_bench PUBLIC ${ENGINE_COMMON_INCLUDES} ${ENGINE_BLOCKCACHE_INCLUDE} ${ENGINE_PRIMPROC_INCLUDE} )
#    target_link_libraries(primitives_scan_bench ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${GTEST_LIBRARIES} processor dbbc benchmark::benchmark)
#    install(TARGETS primitives_scan_bench DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
#endif()

