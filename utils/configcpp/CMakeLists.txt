
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(configcpp_LIB_SRCS configcpp.cpp xmlparser.cpp configstream.cpp)

add_library(configcpp SHARED ${configcpp_LIB_SRCS})
add_dependencies(configcpp loggingcpp)

target_compile_definitions(configcpp PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)

install(TARGETS configcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

