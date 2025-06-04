include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(cplogger_SRCS main.cpp)

columnstore_executable(cplogger ${cplogger_SRCS})

columnstore_link(cplogger ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})
