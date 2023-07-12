
include_directories( ${ENGINE_COMMON_INCLUDES} )
set(datatypes_LIB_SRCS
    mcs_int128.cpp
    mcs_decimal.cpp)

add_library(datatypes STATIC ${datatypes_LIB_SRCS})
add_dependencies(datatypes loggingcpp external_boost)

install(TARGETS datatypes DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

