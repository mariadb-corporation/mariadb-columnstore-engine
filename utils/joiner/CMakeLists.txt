
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(joiner_LIB_SRCS joiner.cpp tuplejoiner.cpp joinpartition.cpp)

add_library(joiner SHARED ${joiner_LIB_SRCS})

set_target_properties(joiner PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS joiner DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


