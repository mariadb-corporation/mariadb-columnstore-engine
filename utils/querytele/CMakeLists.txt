include_directories(${ENGINE_COMMON_INCLUDES})

set(querytele_LIB_SRCS querytele.cpp queryteleclient.cpp querytele_constants.cpp querytele_types.cpp
                       QueryTeleService.cpp queryteleprotoimpl.cpp
)

columnstore_library(querytele ${querytele_LIB_SRCS})
columnstore_link(querytele ${THRIFT_LIBRARY} boost_thread)
target_include_directories(querytele PRIVATE ${THRIFT_INCLUDE_DIRS})

add_dependencies(querytele external_thrift)
