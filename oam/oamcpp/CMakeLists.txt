
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(oamcpp_LIB_SRCS liboamcpp.cpp oamcache.cpp)

add_library(oamcpp SHARED ${oamcpp_LIB_SRCS})

add_dependencies(oamcpp loggingcpp)


# if (CMAKE_CXX_FLAGS MATCHES "-fsanitize=fuzzer" AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
#     message(STATUS "Building oamcpp with fuzzer")
    # target_link_libraries(oamcpp PRIVATE /usr/lib/llvm-16/lib/clang/16/lib/linux/libclang_rt.fuzzer-x86_64.a)

    # target_compile_options(oamcpp PRIVATE -Wno-unused-result -fsanitize=fuzzer)

target_link_libraries(oamcpp PRIVATE /usr/lib/llvm-16/lib/clang/16/lib/linux/libclang_rt.fuzzer-x86_64.a)

target_compile_options(oamcpp PRIVATE -Wno-unused-result -fsanitize=fuzzer)

install(TARGETS oamcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

