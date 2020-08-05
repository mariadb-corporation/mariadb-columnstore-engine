
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(batchloader_LIB_SRCS batchloader.cpp)

add_library(batchloader SHARED ${batchloader_LIB_SRCS})

add_dependencies(batchloader loggingcpp)

target_link_libraries(batchloader ${NETSNMP_LIBRARIES})

install(TARGETS batchloader DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)


