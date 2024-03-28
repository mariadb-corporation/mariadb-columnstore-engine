include_directories( ${ENGINE_COMMON_INCLUDES} )

# from llvm 10
# error: bitwise operation between different enumeration types
# ('FunctionModRefBehavior' and 'llvm::FunctionModRefLocation') is deprecated [-Werror,-Wdeprecated-enum-enum-conversion]
MY_CHECK_AND_SET_COMPILER_FLAG("-Wno-deprecated-enum-enum-conversion")

set(jit_LIB_SRCS
        jit.cpp
        jit.h
        compileoperator.cpp
        compileoperator.h
        expressionjit.h
        compilehelper.h)

add_library(jit SHARED ${jit_LIB_SRCS})
add_dependencies(jit loggingcpp)
target_link_libraries(jit ${LLVM_LIBS} ${LLVM_SYSTEM_LIBS} ${LLVM_LDFLAGS})

install(TARGETS jit DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)