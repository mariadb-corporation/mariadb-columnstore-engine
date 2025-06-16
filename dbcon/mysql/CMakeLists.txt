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
    rulebased_optimizer.cpp
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
    add_to_debian_install_file(${MARIADB_MYCNFDIR}/columnstore.cnf)
    add_to_debian_install_file("${MARIADB_PLUGINDIR}/ha_columnstore.so")

else()
    message("Adding plugin via CMake")
    columnstore_shared_library(ha_columnstore ${libcalmysql_SRCS})
    set_target_properties(ha_columnstore PROPERTIES PREFIX "")

    add_definitions(-DMYSQL_DYNAMIC_PLUGIN -DPLUGIN_COLUMNSTORE_VERSION="${PACKAGE_VERSION}")

    columnstore_link(
        ha_columnstore
        ${S3API_DEPS}
        ${ENGINE_LDFLAGS}
        ${ENGINE_WRITE_LIBS}
        ${NETSNMP_LIBRARIES}
        ${SERVER_BUILD_DIR}/libservices/libmysqlservices.a
        threadpool
        loggingcpp
        marias3
    )
    # define this dummy target for standalone builds (ie, when mysql_add_plugin doesn't exist)
    add_custom_target(columnstore DEPENDS ha_columnstore)
    columnstore_install_file(columnstore.cnf ${MARIADB_MYCNFDIR})
endif()

if(TARGET columnstore)
    columnstore_install_file(syscatalog_mysql.sql ${ENGINE_SUPPORTDIR})
    columnstore_install_file(dumpcat_mysql.sql ${ENGINE_SUPPORTDIR})
    columnstore_install_file(calsetuserpriority.sql ${ENGINE_SUPPORTDIR})
    columnstore_install_file(calremoveuserpriority.sql ${ENGINE_SUPPORTDIR})
    columnstore_install_file(calshowprocesslist.sql ${ENGINE_SUPPORTDIR})
    columnstore_install_file(columnstore_info.sql ${ENGINE_SUPPORTDIR})
    columnstore_install_program(install_mcs_mysql.sh ${ENGINE_SBINDIR})
endif()
