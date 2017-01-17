
include_directories( ${ENGINE_COMMON_INCLUDES} ${SNAPPY_INCLUDE_DIR} )

########### next target ###############

set(compress_LIB_SRCS
    idbcompress.cpp
    version1.cpp)

add_definitions(-DNDEBUG)

add_library(compress SHARED ${compress_LIB_SRCS})

target_link_libraries(compress ${SNAPPY_LIBRARIES})

set_target_properties(compress PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS compress DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

