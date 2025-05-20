include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(oamcpp_LIB_SRCS liboamcpp.cpp oamcache.cpp)

columnstore_library(oamcpp ${oamcpp_LIB_SRCS})

add_dependencies(oamcpp loggingcpp)

target_link_libraries(oamcpp)

target_compile_options(oamcpp PRIVATE -Wno-unused-result)

install(
    TARGETS oamcpp
    DESTINATION ${ENGINE_LIBDIR}
    COMPONENT columnstore-engine
)
