include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(autoConfigure_SRCS autoConfigure.cpp)

add_executable(autoConfigure ${autoConfigure_SRCS})
columnstore_link(autoConfigure ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_EXEC_LIBS})
