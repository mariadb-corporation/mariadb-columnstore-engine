include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(dataconvert_LIB_SRCS dataconvert.cpp)

columnstore_library(dataconvert ${dataconvert_LIB_SRCS})

add_dependencies(dataconvert loggingcpp)

install(TARGETS dataconvert DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
