
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(joiner_LIB_SRCS joiner.cpp tuplejoiner.cpp joinpartition.cpp)

add_library(joiner SHARED ${joiner_LIB_SRCS})

install(TARGETS joiner DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)


