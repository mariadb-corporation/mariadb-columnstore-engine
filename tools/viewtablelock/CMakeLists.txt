include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(viewtablelock_SRCS viewtablelock.cpp)

columnstore_executable(viewtablelock ${viewtablelock_SRCS})
columnstore_link(viewtablelock ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})
