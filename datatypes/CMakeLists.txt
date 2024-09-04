INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})
SET (datatypes_LIB_SRCS mcs_int128.cpp mcs_decimal.cpp)

ADD_LIBRARY (datatypes SHARED ${datatypes_LIB_SRCS})
ADD_DEPENDENCIES (datatypes loggingcpp external_boost)

INSTALL (
  TARGETS datatypes
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
