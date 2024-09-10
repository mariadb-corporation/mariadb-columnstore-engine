INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (writeengineredistribute_LIB_SRCS we_redistribute.cpp we_redistributecontrol.cpp we_redistributecontrolthread.cpp
                                      we_redistributeworkerthread.cpp
)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (writeengineredistribute STATIC ${writeengineredistribute_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (writeengineredistribute SHARED ${writeengineredistribute_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (writeengineredistribute loggingcpp)

TARGET_LINK_LIBRARIES (writeengineredistribute ${NETSNMP_LIBRARIES})

TARGET_COMPILE_DEFINITIONS (writeengineredistribute PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)

INSTALL (
  TARGETS writeengineredistribute
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
