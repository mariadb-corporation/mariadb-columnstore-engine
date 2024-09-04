INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (configcpp_LIB_SRCS configcpp.cpp xmlparser.cpp configstream.cpp)

ADD_LIBRARY (configcpp SHARED ${configcpp_LIB_SRCS})
ADD_DEPENDENCIES (configcpp loggingcpp)

TARGET_COMPILE_DEFINITIONS (configcpp PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)

INSTALL (
  TARGETS configcpp
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
