INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (querytele_LIB_SRCS querytele.cpp queryteleclient.cpp querytele_constants.cpp querytele_types.cpp
                        QueryTeleService.cpp queryteleprotoimpl.cpp
)

ADD_LIBRARY (querytele SHARED ${querytele_LIB_SRCS})
ADD_DEPENDENCIES (querytele external_boost external_thrift)

TARGET_INCLUDE_DIRECTORIES (querytele PRIVATE ${THRIFT_INCLUDE_DIRS})
TARGET_LINK_LIBRARIES (querytele ${THRIFT_LIBRARY})
INSTALL (
  TARGETS querytele
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
