include_directories( ${ENGINE_COMMON_INCLUDES} )

# from llvm 10
# error: bitwise operation between different enumeration types
# ('FunctionModRefBehavior' and 'llvm::FunctionModRefLocation') is deprecated [-Werror,-Wdeprecated-enum-enum-conversion]
# non-virtual-dtor and missing-braces come from compilation errors with llvm 16.
#MY_CHECK_AND_SET_COMPILER_FLAG("-Wno-deprecated-enum-enum-conversion ")
string(REPLACE "-Werror " " " CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-braces -Wno-non-virtual-dtor")

set(jit_LIB_SRCS
        jit.cpp
        jit.h
        modules_storage.cpp
        compileoperator.cpp
        compileoperator.h
        expressionjit.h
        compilehelper.h)

add_library(jit SHARED ${jit_LIB_SRCS})
add_dependencies(jit loggingcpp)
target_link_libraries(jit ${LLVM_LIBS} ${LLVM_SYSTEM_LIBS} ${LLVM_LDFLAGS})

install(TARGETS jit DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)