include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(joiner_LIB_SRCS tuplejoiner.cpp joinpartition.cpp)

columnstore_library(joiner ${joiner_LIB_SRCS})

add_dependencies(joiner loggingcpp)

install(TARGETS joiner DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
