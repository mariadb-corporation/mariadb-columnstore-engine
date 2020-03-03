
include_directories( ${ENGINE_COMMON_INCLUDES} )
set(datatypes_LIB_SRCS
    decimal.cpp)

add_library(datatypes SHARED ${datatypes_LIB_SRCS})

install(TARGETS datatypes DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-libs)

