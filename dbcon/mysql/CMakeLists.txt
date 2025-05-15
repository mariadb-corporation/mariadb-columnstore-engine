configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/install_mcs_mysql.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/install_mcs_mysql.sh" @ONLY
)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/ha_mcs_version.h.in" "${CMAKE_CURRENT_SOURCE_DIR}/ha_mcs_version.h")

include_directories(${ENGINE_COMMON_INCLUDES} /usr/include/libxml2 ${SERVER_SOURCE_ROOT_DIR}/storage/maria)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../../../)
set(S3API_DEPS
    marias3 curl
    CACHE INTERNAL "S3API_DEPS"
)

set(libcalmysql_SRCS
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

set_source_files_properties(ha_mcs.cpp PROPERTIES COMPILE_FLAGS "-fno-implicit-templates")

if(COMMAND mysql_add_plugin)
    if(NOT (RPM OR DEB))
        set(disabled DISABLED)
    endif()
    message("Adding plugin via MariaDB plugin API")
    mysql_add_plugin(
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
        ${MARIADB_CLIENT_LIBS}
        ${S3API_DEPS}
        threadpool
        VERSION
        ${PACKAGE_VERSION}
        COMPONENT
        columnstore-engine
        CONFIG
        columnstore.cnf
    )
else()
    message("Adding plugin via CMake")
    add_library(ha_columnstore SHARED ${libcalmysql_SRCS})
    set_target_properties(ha_columnstore PROPERTIES PREFIX "")

    add_dependencies(ha_columnstore loggingcpp)
    add_dependencies(ha_columnstore marias3)
    add_definitions(-DMYSQL_DYNAMIC_PLUGIN -DPLUGIN_COLUMNSTORE_VERSION="${PACKAGE_VERSION}")

    target_link_libraries(
        ha_columnstore
        ${S3API_DEPS}
        ${ENGINE_LDFLAGS}
        ${ENGINE_WRITE_LIBS}
        ${NETSNMP_LIBRARIES}
        ${SERVER_BUILD_DIR}/libservices/libmysqlservices.a
        threadpool
    )

    install(
        TARGETS ha_columnstore
        DESTINATION ${MARIADB_PLUGINDIR}
        COMPONENT columnstore-engine
    )

    # define this dummy target for standalone builds (ie, when mysql_add_plugin doesn't exist)
    add_custom_target(columnstore DEPENDS ha_columnstore)
    install(
        FILES columnstore.cnf
        DESTINATION ${MARIADB_MYCNFDIR}
        COMPONENT columnstore-engine
    )
endif()

if(TARGET columnstore)
    install(
        FILES syscatalog_mysql.sql dumpcat_mysql.sql calsetuserpriority.sql calremoveuserpriority.sql
              calshowprocesslist.sql columnstore_info.sql
        DESTINATION ${ENGINE_SUPPORTDIR}
        COMPONENT columnstore-engine
    )
    install(
        PROGRAMS install_mcs_mysql.sh
        DESTINATION ${ENGINE_SBINDIR}
        COMPONENT columnstore-engine
    )
endif()
