find_path(LZ4_ROOT_DIR
    NAMES include/lz4.h
)

find_library(LZ4_LIBRARIES
    NAMES lz4
    HINTS ${LZ4_ROOT_DIR}/lib
)

find_path(LZ4_INCLUDE_DIR
    NAMES lz4.h
    HINTS ${LZ4_ROOT_DIR}/include
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(lz4 DEFAULT_MSG
    LZ4_LIBRARIES
    LZ4_INCLUDE_DIR
)

mark_as_advanced(
    LZ4_ROOT_DIR
    LZ4_LIBRARIES
    LZ4_INCLUDE_DIR
)
