INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES} ${ENGINE_BLOCKCACHE_INCLUDE} ${ENGINE_PRIMPROC_INCLUDE})

IF (NOT WITH_UBSAN AND NOT WITH_TSAN)
  MY_CHECK_AND_SET_COMPILER_FLAG ("-U_FORTIFY_SOURCE" DEBUG RELWITHDEBINFO)
  MY_CHECK_AND_SET_COMPILER_FLAG ("-fsanitize=address -fsanitize-address-use-after-scope -fPIC")
ENDIF ()

IF (WITH_UNITTESTS)
  CMAKE_POLICY (SET CMP0054 NEW)
  SET (EXTERNAL_INSTALL_LOCATION ${CMAKE_BINARY_DIR}/external)
  EXTERNALPROJECT_ADD (
    googletest
    GIT_REPOSITORY https://github.com/google/googletest
    GIT_TAG release-1.12.0
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION} -DBUILD_SHARED_LIBS=ON
  )

  # this is a workaround for gtest's bug still not closed https://github.com/google/googletest/issues/3659
  IF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    MY_CHECK_AND_SET_COMPILER_FLAG ("-U__cpp_char8_t")
  ENDIF ()

  INCLUDE_DIRECTORIES (${EXTERNAL_INSTALL_LOCATION}/include)
  # lib64 for RPM-based distros
  LINK_DIRECTORIES (${EXTERNAL_INSTALL_LOCATION}/lib ${EXTERNAL_INSTALL_LOCATION}/lib64)
  SET (GTEST_LIBRARIES gtest gtest_main pthread)
  INCLUDE (GoogleTest)

  # GoogleTest tests
  ADD_EXECUTABLE (rowgroup_tests rowgroup-tests.cpp)
  ADD_DEPENDENCIES (rowgroup_tests googletest)
  TARGET_LINK_LIBRARIES (rowgroup_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS})
  GTEST_ADD_TESTS (TARGET rowgroup_tests TEST_PREFIX columnstore:)

  ADD_EXECUTABLE (rewritetest rewritetest.cpp)
  ADD_DEPENDENCIES (rewritetest googletest)
  TARGET_LINK_LIBRARIES (rewritetest ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS} messageqcpp execplan)
  GTEST_ADD_TESTS (TARGET rewritetest TEST_PREFIX columnstore:)

  ADD_EXECUTABLE (mcs_decimal_tests mcs_decimal-tests.cpp)
  ADD_DEPENDENCIES (mcs_decimal_tests googletest)
  TARGET_LINK_LIBRARIES (mcs_decimal_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS})
  GTEST_ADD_TESTS (TARGET mcs_decimal_tests TEST_PREFIX columnstore:)

  ADD_EXECUTABLE (dataconvert_tests dataconvert-tests.cpp)
  ADD_DEPENDENCIES (dataconvert_tests googletest)
  TARGET_LINK_LIBRARIES (dataconvert_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS})
  GTEST_ADD_TESTS (TARGET dataconvert_tests TEST_PREFIX columnstore:)

  ADD_EXECUTABLE (rebuild_em_tests rebuild-em-tests.cpp)
  ADD_DEPENDENCIES (rebuild_em_tests googletest)
  TARGET_LINK_LIBRARIES (rebuild_em_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_WRITE_LIBS})
  GTEST_ADD_TESTS (TARGET rebuild_em_tests TEST_PREFIX columnstore:)

  ADD_EXECUTABLE (compression_tests compression-tests.cpp)
  ADD_DEPENDENCIES (compression_tests googletest)
  TARGET_LINK_LIBRARIES (compression_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_WRITE_LIBS})
  GTEST_ADD_TESTS (TARGET compression_tests TEST_PREFIX columnstore:)

  ADD_EXECUTABLE (column_scan_filter_tests primitives_column_scan_and_filter.cpp)
  TARGET_COMPILE_OPTIONS (column_scan_filter_tests PRIVATE -Wno-error -Wno-sign-compare)
  ADD_DEPENDENCIES (column_scan_filter_tests googletest)
  TARGET_LINK_LIBRARIES (
    column_scan_filter_tests ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${GTEST_LIBRARIES} processor dbbc
  )
  GTEST_ADD_TESTS (TARGET column_scan_filter_tests TEST_PREFIX columnstore:)

  ADD_EXECUTABLE (simd_processors simd_processors.cpp)
  TARGET_COMPILE_OPTIONS (simd_processors PRIVATE -Wno-error)
  ADD_DEPENDENCIES (simd_processors googletest)
  TARGET_LINK_LIBRARIES (simd_processors ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${GTEST_LIBRARIES} processor dbbc)
  GTEST_ADD_TESTS (TARGET simd_processors TEST_PREFIX columnstore:)

  ADD_EXECUTABLE (fair_threadpool_test fair_threadpool.cpp)
  ADD_DEPENDENCIES (fair_threadpool_test googletest)
  TARGET_LINK_LIBRARIES (fair_threadpool_test ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${GTEST_LIBRARIES} processor dbbc)
  GTEST_ADD_TESTS (TARGET fair_threadpool_test TEST_PREFIX columnstore:)

  ADD_EXECUTABLE (comparators_tests comparators-tests.cpp)
  TARGET_LINK_LIBRARIES (comparators_tests ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
  ADD_TEST (NAME columnstore:comparators_tests COMMAND comparators_tests)

  # standalone EM routines test
  ADD_EXECUTABLE (brm_em_standalone brm-em-standalone.cpp)
  TARGET_LINK_LIBRARIES (brm_em_standalone ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
  ADD_TEST (NAME columnstore:brm_em_standalone COMMAND brm_em_standalone)
  SET_TESTS_PROPERTIES (columnstore:brm_em_standalone PROPERTIES DISABLED True)
ENDIF ()

IF (WITH_MICROBENCHMARKS AND (NOT CMAKE_BUILD_TYPE STREQUAL "Debug"))
  FIND_PACKAGE (benchmark REQUIRED)
  ADD_EXECUTABLE (primitives_scan_bench primitives_scan_bench.cpp)
  TARGET_INCLUDE_DIRECTORIES (
    primitives_scan_bench PUBLIC ${ENGINE_COMMON_INCLUDES} ${ENGINE_BLOCKCACHE_INCLUDE} ${ENGINE_PRIMPROC_INCLUDE}
  )
  TARGET_LINK_LIBRARIES (
    primitives_scan_bench
    ${ENGINE_LDFLAGS}
    ${ENGINE_WRITE_LIBS}
    ${GTEST_LIBRARIES}
    processor
    dbbc
    benchmark::benchmark
  )
  ADD_TEST (NAME columnstore_microbenchmarks:primitives_scan_bench, COMMAND primitives_scan_bench)
ENDIF ()
