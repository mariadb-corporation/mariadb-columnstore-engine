include_directories(${ENGINE_COMMON_INCLUDES} ${SNAPPY_INCLUDE_DIR})

# hack for lz4 duplicate header
get_property(
    dirs
    DIRECTORY
    PROPERTY INCLUDE_DIRECTORIES
)
list(REMOVE_ITEM dirs ${CMAKE_SOURCE_DIR}/include/providers)
set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES "${dirs}")

set(compress_LIB_SRCS idbcompress.cpp)

columnstore_library(compress ${compress_LIB_SRCS})
columnstore_link(compress PRIVATE loggingcpp boost_filesystem ${SNAPPY_LIBRARIES})

if(HAVE_LZ4)
    message_once(STATUS "LINK WITH LZ4")
    columnstore_link(compress PRIVATE ${LZ4_LIBRARIES})
endif()
