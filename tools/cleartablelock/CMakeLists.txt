include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(cleartablelock_SRCS cleartablelock.cpp cleartablelockthread.cpp)

columnstore_executable(cleartablelock ${cleartablelock_SRCS})

columnstore_link(cleartablelock ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_WRITE_LIBS})
