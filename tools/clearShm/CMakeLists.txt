include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(clearShm_SRCS main.cpp)

columnstore_executable(clearShm ${clearShm_SRCS})

columnstore_link(clearShm ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})
