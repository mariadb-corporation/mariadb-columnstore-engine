include_directories( ${ENGINE_COMMON_INCLUDES} )


set(jit_LIB_SRCS
        jit.cpp
        jit.h
        compileoperator.cpp
        compileoperator.h
        expressionjit.h)


add_library(jit SHARED ${jit_LIB_SRCS})

add_dependencies(jit loggingcpp)

target_link_libraries(jit ${NETSNMP_LIBRARIES} ${LLVM_LIBS} ${LLVM_SYSTEM_LIBS} ${LLVM_LDFLAGS})

install(TARGETS jit DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)