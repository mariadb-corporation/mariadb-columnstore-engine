INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (autoConfigure_SRCS autoConfigure.cpp)

ADD_EXECUTABLE (autoConfigure ${autoConfigure_SRCS})

TARGET_LINK_LIBRARIES (autoConfigure ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_EXEC_LIBS})

# install(TARGETS autoConfigure DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
