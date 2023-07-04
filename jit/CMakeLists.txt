set(CMAKE_CXX_STANDARD 14)


set(jit_LIB_SRCS
        jit.cpp
        jit.h
        compilefunction.cpp compilefunction.h)


add_library(jit SHARED ${jit_LIB_SRCS})

find_package(LLVM REQUIRED CONFIG)

message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")

# Set your project compile flags.
# E.g. if using the C++ header files
# you will need to enable C++11 support
# for your compiler.

include_directories(${LLVM_INCLUDE_DIRS})
separate_arguments(LLVM_DEFINITIONS_LIST NATIVE_COMMAND ${LLVM_DEFINITIONS})
add_definitions(${LLVM_DEFINITIONS_LIST})

# Find the libraries that correspond to the LLVM components
# that we wish to use
llvm_map_components_to_libnames(llvm_libs support core irreader)

# Link against LLVM libraries
target_link_libraries(jit ${llvm_libs})

install(TARGETS jit DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

