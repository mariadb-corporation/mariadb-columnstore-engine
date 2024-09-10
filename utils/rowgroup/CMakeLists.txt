INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (rowgroup_LIB_SRCS rowaggregation.cpp rowgroup.cpp rowstorage.cpp)

# librowgroup_la_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (rowgroup STATIC ${rowgroup_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (rowgroup SHARED ${rowgroup_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (rowgroup loggingcpp)

TARGET_LINK_LIBRARIES (rowgroup ${NETSNMP_LIBRARIES} funcexp)

INSTALL (
  TARGETS rowgroup
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
