INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

SET (rebuildEM_SRCS main.cpp rebuildEM.cpp)
ADD_EXECUTABLE (mcsRebuildEM ${rebuildEM_SRCS})
TARGET_LINK_LIBRARIES (mcsRebuildEM ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} boost_system boost_filesystem)
INSTALL (
  TARGETS mcsRebuildEM
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
