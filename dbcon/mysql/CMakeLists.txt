CONFIGURE_FILE (
  "${CMAKE_CURRENT_SOURCE_DIR}/install_mcs_mysql.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/install_mcs_mysql.sh" @ONLY
)
CONFIGURE_FILE ("${CMAKE_CURRENT_SOURCE_DIR}/ha_mcs_version.h.in" "${CMAKE_CURRENT_SOURCE_DIR}/ha_mcs_version.h")

INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES} /usr/include/libxml2 ${SERVER_SOURCE_ROOT_DIR}/storage/maria)

SET (CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../../../)
SET (
  S3API_DEPS
  marias3 curl
  CACHE INTERNAL "S3API_DEPS"
)

SET (
  libcalmysql_SRCS
  ../../datatypes/mcs_datatype.cpp
  ../../utils/libmysql_client/libmysql_client.cpp
  ../../utils/querystats/querystats.cpp
  ha_mcs_sysvars.cpp
  ha_mcs_client_udfs.cpp
  ha_mcs_opt_rewrites.cpp
  ha_mcs_pushdown.cpp
  ha_mcs.cpp
  ha_mcs_impl.cpp
  ha_mcs_dml.cpp
  ha_mcs_ddl.cpp
  ha_mcs_execplan.cpp
  ha_scalar_sub.cpp
  ha_in_sub.cpp
  ha_exists_sub.cpp
  ha_from_sub.cpp
  ha_select_sub.cpp
  ha_view.cpp
  sm.cpp
  ha_window_function.cpp
  ha_mcs_partition.cpp
  ha_pseudocolumn.cpp
  ha_tzinfo.cpp
  is_columnstore_tables.cpp
  is_columnstore_columns.cpp
  is_columnstore_files.cpp
  is_columnstore_extents.cpp
  columnstore_dataload.cpp
)

SET_SOURCE_FILES_PROPERTIES (ha_mcs.cpp PROPERTIES COMPILE_FLAGS "-fno-implicit-templates")

IF (COMMAND mysql_add_plugin)
  IF (NOT (RPM OR DEB))
    SET (disabled DISABLED)
  ENDIF ()

  MYSQL_ADD_PLUGIN (
    columnstore
    ${libcalmysql_SRCS}
    STORAGE_ENGINE
    MODULE_ONLY
    ${disabled}
    LINK_LIBRARIES
    ${ENGINE_LDFLAGS}
    ${PLUGIN_EXEC_LIBS}
    ${PLUGIN_WRITE_LIBS}
    joblist_server
    ${NETSNMP_LIBRARIES}
    ${S3API_DEPS}
    threadpool
    VERSION
    ${PACKAGE_VERSION}
    COMPONENT
    columnstore-engine
    CONFIG
    columnstore.cnf
  )
ELSE ()
  ADD_LIBRARY (ha_columnstore SHARED ${libcalmysql_SRCS})
  SET_TARGET_PROPERTIES (ha_columnstore PROPERTIES PREFIX "")

  ADD_DEPENDENCIES (ha_columnstore loggingcpp)
  ADD_DEPENDENCIES (ha_columnstore marias3)
  ADD_DEFINITIONS (-DMYSQL_DYNAMIC_PLUGIN -DPLUGIN_COLUMNSTORE_VERSION="${PACKAGE_VERSION}")

  TARGET_LINK_LIBRARIES (
    ha_columnstore
    ${S3API_DEPS}
    ${ENGINE_LDFLAGS}
    ${ENGINE_WRITE_LIBS}
    ${NETSNMP_LIBRARIES}
    ${SERVER_BUILD_DIR}/libservices/libmysqlservices.a
    threadpool
  )

  INSTALL (
    TARGETS ha_columnstore
    DESTINATION ${MARIADB_PLUGINDIR}
    COMPONENT columnstore-engine
  )

  # define this dummy target for standalone builds (ie, when mysql_add_plugin doesn't exist)
  ADD_CUSTOM_TARGET (columnstore DEPENDS ha_columnstore)
  INSTALL (
    FILES columnstore.cnf
    DESTINATION ${MARIADB_MYCNFDIR}
    COMPONENT columnstore-engine
  )
ENDIF ()

IF (TARGET columnstore)
  INSTALL (
    FILES syscatalog_mysql.sql dumpcat_mysql.sql calsetuserpriority.sql calremoveuserpriority.sql calshowprocesslist.sql
          columnstore_info.sql
    DESTINATION ${ENGINE_SUPPORTDIR}
    COMPONENT columnstore-engine
  )
  INSTALL (
    PROGRAMS install_mcs_mysql.sh
    DESTINATION ${ENGINE_SBINDIR}
    COMPONENT columnstore-engine
  )
ENDIF ()
