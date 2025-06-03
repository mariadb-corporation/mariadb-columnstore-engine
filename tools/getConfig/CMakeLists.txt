include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(getConfig_SRCS main.cpp)

columnstore_executable(mcsGetConfig ${getConfig_SRCS})

columnstore_link(mcsGetConfig ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})
