
include_directories( ${ENGINE_COMMON_INCLUDES} ${SNAPPY_INCLUDE_DIR} )

########### next target ###############

set(compress_LIB_SRCS
    idbcompress.cpp)

add_definitions(-DNDEBUG)

add_library(compress SHARED ${compress_LIB_SRCS})
add_dependencies(compress loggingcpp)

target_link_libraries(compress ${SNAPPY_LIBRARIES})

install(TARGETS compress DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

