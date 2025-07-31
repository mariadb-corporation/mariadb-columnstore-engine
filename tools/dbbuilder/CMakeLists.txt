include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(dbbuilder_SRCS dbbuilder.cpp systemcatalog.cpp)

columnstore_executable(dbbuilder ${dbbuilder_SRCS})

columnstore_link(dbbuilder ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_WRITE_LIBS})
