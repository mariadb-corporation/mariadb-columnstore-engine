INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (clearShm_SRCS main.cpp)

ADD_EXECUTABLE (clearShm ${clearShm_SRCS})

TARGET_LINK_LIBRARIES (clearShm ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS clearShm
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
