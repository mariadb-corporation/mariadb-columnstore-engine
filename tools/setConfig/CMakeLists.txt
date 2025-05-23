include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(setConfig_SRCS main.cpp)

columnstore_executable(mcsSetConfig ${setConfig_SRCS})
columnstore_link(mcsSetConfig ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_EXEC_LIBS})
