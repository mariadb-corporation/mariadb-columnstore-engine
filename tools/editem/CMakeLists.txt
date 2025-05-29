include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(editem_SRCS editem.cpp)

columnstore_executable(editem ${editem_SRCS})
columnstore_link(editem ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})
