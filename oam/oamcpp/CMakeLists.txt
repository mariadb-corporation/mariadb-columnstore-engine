INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (oamcpp_LIB_SRCS liboamcpp.cpp oamcache.cpp)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (oamcpp STATIC ${oamcpp_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (oamcpp SHARED ${oamcpp_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (oamcpp loggingcpp)

TARGET_LINK_LIBRARIES (oamcpp)

TARGET_COMPILE_OPTIONS (oamcpp PRIVATE -Wno-unused-result)

INSTALL (
  TARGETS oamcpp
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
