INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (
  ddlpackageproc_LIB_SRCS
  ddlpackageprocessor.cpp
  createtableprocessor.cpp
  altertableprocessor.cpp
  droptableprocessor.cpp
  markpartitionprocessor.cpp
  restorepartitionprocessor.cpp
  droppartitionprocessor.cpp
)

ADD_LIBRARY (ddlpackageproc SHARED ${ddlpackageproc_LIB_SRCS})

ADD_DEPENDENCIES (ddlpackageproc loggingcpp)

TARGET_LINK_LIBRARIES (ddlpackageproc ${NETSNMP_LIBRARIES})

INSTALL (
  TARGETS ddlpackageproc
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
