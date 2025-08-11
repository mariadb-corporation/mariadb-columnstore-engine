include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(rgprint_SRCS rgprint.cpp)

columnstore_executable(rgprint ${rgprint_SRCS})

columnstore_link(rgprint ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_WRITE_LIBS})
