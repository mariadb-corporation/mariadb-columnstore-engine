
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(oamcpp_LIB_SRCS liboamcpp.cpp oamcache.cpp)

add_library(oamcpp SHARED ${oamcpp_LIB_SRCS})

target_link_libraries(oamcpp )

set_target_properties(oamcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS oamcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


