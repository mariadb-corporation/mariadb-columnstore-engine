
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(joiner_LIB_SRCS tuplejoiner.cpp joinpartition.cpp)

add_library(joiner SHARED ${joiner_LIB_SRCS})

add_dependencies(joiner loggingcpp)

install(TARGETS joiner DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)


