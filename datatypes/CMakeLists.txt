INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})
SET (datatypes_LIB_SRCS mcs_int128.cpp mcs_decimal.cpp)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (datatypes STATIC ${datatypes_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (datatypes SHARED ${datatypes_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (datatypes loggingcpp external_boost)

INSTALL (
  TARGETS datatypes
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
