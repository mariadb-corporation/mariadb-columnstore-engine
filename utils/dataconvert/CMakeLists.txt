
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(dataconvert_LIB_SRCS dataconvert.cpp)

add_library(dataconvert STATIC ${dataconvert_LIB_SRCS})

add_dependencies(dataconvert loggingcpp)
target_link_libraries(dataconvert loggingcpp common)

install(TARGETS dataconvert DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
