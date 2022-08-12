
include_directories( ${ENGINE_COMMON_INCLUDES} ${SNAPPY_INCLUDE_DIR} )

#hack for lz4 duplicate header
GET_PROPERTY(dirs DIRECTORY PROPERTY INCLUDE_DIRECTORIES)
LIST(REMOVE_ITEM dirs ${CMAKE_SOURCE_DIR}/include/providers)
SET_PROPERTY(DIRECTORY PROPERTY INCLUDE_DIRECTORIES "${dirs}")

set(compress_LIB_SRCS
    idbcompress.cpp)

add_definitions(-DNDEBUG)

add_library(compress SHARED ${compress_LIB_SRCS})
add_dependencies(compress loggingcpp external_boost)

target_link_libraries(compress ${SNAPPY_LIBRARIES})
IF(HAVE_LZ4)
    MESSAGE_ONCE(STATUS "LINK WITH LZ4")
    target_link_libraries(compress ${LZ4_LIBRARIES})
ENDIF()

install(TARGETS compress DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
