include_directories( ${ENGINE_COMMON_INCLUDES} )


set(jit_LIB_SRCS
        jit.cpp
        jit.h
        compilefunction.cpp
        compilefunction.h
        compileoperator.cpp
        compileoperator.h
        expressionjit.h)


add_library(jit SHARED ${jit_LIB_SRCS})

target_link_libraries(jit ${NETSNMP_LIBRARIES} ${LLVM_LIBS} ${LLVM_SYSTEM_LIBS} ${LLVM_LDFLAGS})

install(TARGETS jit DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)