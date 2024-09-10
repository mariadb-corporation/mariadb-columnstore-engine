INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (configcpp_LIB_SRCS configcpp.cpp xmlparser.cpp configstream.cpp)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (configcpp STATIC ${configcpp_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (configcpp SHARED ${configcpp_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (configcpp loggingcpp)

TARGET_COMPILE_DEFINITIONS (configcpp PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)

INSTALL (
  TARGETS configcpp
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
