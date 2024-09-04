INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (
  writeengine_LIB_SRCS
  writeengine.cpp
  we_colop.cpp
  we_colopcompress.cpp
  we_dctnrycompress.cpp
  we_tablemetadata.cpp
  ../shared/we_blockop.cpp
  ../shared/we_brm.cpp
  ../shared/we_cache.cpp
  ../shared/we_chunkmanager.cpp
  ../shared/we_config.cpp
  ../shared/we_convertor.cpp
  ../shared/we_dbfileop.cpp
  ../shared/we_define.cpp
  ../shared/we_fileop.cpp
  ../shared/we_log.cpp
  ../shared/we_stats.cpp
  ../shared/we_bulkrollbackmgr.cpp
  ../shared/we_simplesyslog.cpp
  ../shared/we_bulkrollbackfilecompressed.cpp
  ../shared/we_bulkrollbackfilecompressedhdfs.cpp
  ../shared/we_bulkrollbackfile.cpp
  ../shared/we_rbmetawriter.cpp
  ../shared/we_dbrootextenttracker.cpp
  ../shared/we_confirmhdfsdbfile.cpp
  ../dictionary/we_dctnry.cpp
  ../xml/we_xmlop.cpp
  ../xml/we_xmljob.cpp
  ../xml/we_xmlgendata.cpp
  ../xml/we_xmlgenproc.cpp
)

ADD_DEFINITIONS (-D_FILE_OFFSET_BITS=64)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (writeengine STATIC ${writeengine_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (writeengine SHARED ${writeengine_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (writeengine loggingcpp)

TARGET_LINK_LIBRARIES (writeengine ${NETSNMP_LIBRARIES} arrow)

INSTALL (
  TARGETS writeengine
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
