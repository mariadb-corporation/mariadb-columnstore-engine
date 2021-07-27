include_directories( ${ENGINE_COMMON_INCLUDES} )

if (WITH_UNITTESTS)
    enable_testing()
    include(GoogleTest)
    find_package(GTest REQUIRED)

    #GoogleTest tests
    add_executable(rowgroup_tests rowgroup-tests.cpp)
    target_link_libraries(rowgroup_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS})
    gtest_discover_tests(rowgroup_tests TEST_PREFIX columnstore:)

    add_executable(arithmeticoperator_tests arithmeticoperator-tests.cpp)
    target_link_libraries(arithmeticoperator_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS})
    gtest_discover_tests(arithmeticoperator_tests TEST_PREFIX columnstore:)

    add_executable(mcs_decimal_tests mcs_decimal-tests.cpp)
    target_link_libraries(mcs_decimal_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS})
    gtest_discover_tests(mcs_decimal_tests TEST_PREFIX columnstore:)

    add_executable(dataconvert_tests dataconvert-tests.cpp)
    target_link_libraries(dataconvert_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS})
    gtest_discover_tests(dataconvert_tests TEST_PREFIX columnstore:)

    add_executable(rebuild_em_tests rebuild-em-tests.cpp)
    target_link_libraries(rebuild_em_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS})
    gtest_discover_tests(rebuild_em_tests TEST_PREFIX columnstore:)

    add_executable(compression_tests compression-tests.cpp)
    target_link_libraries(compression_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS})
    gtest_discover_tests(compression_tests TEST_PREFIX columnstore:)

    # CPPUNIT TESTS
    add_executable(we_shared_components_tests shared_components_tests.cpp)
    add_dependencies(we_shared_components_tests loggingcpp)
    target_link_libraries(we_shared_components_tests ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} cppunit)
    #Fix Test and uncomment
    #add_test(NAME columnstore:we_shared_components_tests, COMMAND we_shared_components_tests)

    add_executable(comparators_tests comparators-tests.cpp)
    target_link_libraries(comparators_tests ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
    add_test(NAME columnstore:comparators_tests, COMMAND comparators_tests)
endif()
