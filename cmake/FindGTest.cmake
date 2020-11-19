find_path(GTEST_ROOT_DIR
    NAMES include/gtest/gtest.h
)

find_library(GTEST_LIBRARY
    NAMES gtest 
    HINTS ${GTEST_ROOT_DIR}/lib
)

find_library(GTESTMAIN_LIBRARY
    NAMES gtest_main 
    HINTS ${GTEST_ROOT_DIR}/lib
)

find_library(PTHREAD_LIBRARY
    NAMES pthread 
    HINTS ${GTEST_ROOT_DIR}/lib
)

find_path(GTEST_INCLUDE_DIR
    NAMES gtest.h
    HINTS ${GTEST_ROOT_DIR}/include/gtest
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GTest DEFAULT_MSG
    GTEST_LIBRARY
    GTESTMAIN_LIBRARY
    PTHREAD_LIBRARY
    GTEST_INCLUDE_DIR
)

mark_as_advanced(
    GTEST_ROOT_DIR
    GTEST_LIBRARIES
    GTEST_INCLUDE_DIR
)
