INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (idbmeminfo_SRCS idbmeminfo.cpp)

ADD_EXECUTABLE (idbmeminfo ${idbmeminfo_SRCS})

TARGET_LINK_LIBRARIES (idbmeminfo ${ENGINE_LDFLAGS})

INSTALL (
  TARGETS idbmeminfo
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
