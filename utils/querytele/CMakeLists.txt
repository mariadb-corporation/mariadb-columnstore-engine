
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(querytele_LIB_SRCS
    querytele.cpp
    queryteleclient.cpp
    querytele_constants.cpp
    querytele_types.cpp
    QueryTeleService.cpp
    queryteleprotoimpl.cpp)

add_library(querytele STATIC ${querytele_LIB_SRCS})
add_dependencies(querytele external_boost external_thrift)

target_include_directories(querytele PRIVATE ${THRIFT_INCLUDE_DIRS})
target_link_libraries(querytele ${THRIFT_LIBRARY})
install(TARGETS querytele DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

