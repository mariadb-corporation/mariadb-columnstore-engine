INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (
  dmlpackageproc_LIB_SRCS
  deletepackageprocessor.cpp
  dmlpackageprocessor.cpp
  insertpackageprocessor.cpp
  updatepackageprocessor.cpp
  commandpackageprocessor.cpp
  autoincrementdata.cpp
  tablelockdata.cpp
)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (dmlpackageproc STATIC ${dmlpackageproc_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (dmlpackageproc SHARED ${dmlpackageproc_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (dmlpackageproc loggingcpp)

TARGET_LINK_LIBRARIES (dmlpackageproc ${NETSNMP_LIBRARIES})

INSTALL (
  TARGETS dmlpackageproc
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
