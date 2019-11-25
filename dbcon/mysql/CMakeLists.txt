configure_file("${CMAKE_CURRENT_SOURCE_DIR}/install_mcs_mysql.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/install_mcs_mysql.sh" @ONLY)


include_directories( ${ENGINE_COMMON_INCLUDES}
                     /usr/include/libxml2 )


SET ( libcalmysql_SRCS
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
    ha_view.cpp sm.cpp
    ha_window_function.cpp
    ha_mcs_partition.cpp
    ha_pseudocolumn.cpp)

add_definitions(-DMYSQL_DYNAMIC_PLUGIN)

set_source_files_properties(ha_mcs.cpp PROPERTIES COMPILE_FLAGS "-fno-implicit-templates")

add_library(ha_columnstore SHARED ${libcalmysql_SRCS})
SET_TARGET_PROPERTIES(ha_columnstore PROPERTIES PREFIX "")

target_link_libraries(ha_columnstore ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${NETSNMP_LIBRARIES} ${SERVER_BUILD_DIR}/libservices/libmysqlservices.a threadpool)

set_target_properties(ha_columnstore PROPERTIES VERSION 1.0.0 SOVERSION 1)

SET ( is_columnstore_tables_SRCS
    is_columnstore_tables.cpp
    sm.cpp
    )
add_library(is_columnstore_tables SHARED ${is_columnstore_tables_SRCS})

target_link_libraries(is_columnstore_tables ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${SERVER_BUILD_DIR}/libservices/libmysqlservices.a threadpool)

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_tables PROPERTIES PREFIX "")
set_target_properties(is_columnstore_tables PROPERTIES VERSION 1.0.0 SOVERSION 1)

SET ( is_columnstore_columns_SRCS
    is_columnstore_columns.cpp
    sm.cpp
    )
add_library(is_columnstore_columns SHARED ${is_columnstore_columns_SRCS})

target_link_libraries(is_columnstore_columns ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${SERVER_BUILD_DIR}/libservices/libmysqlservices.a threadpool)

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_columns PROPERTIES PREFIX "")
set_target_properties(is_columnstore_columns PROPERTIES VERSION 1.0.0 SOVERSION 1)

SET ( is_columnstore_extents_SRCS
    is_columnstore_extents.cpp
    sm.cpp
    )
add_library(is_columnstore_extents SHARED ${is_columnstore_extents_SRCS})

target_link_libraries(is_columnstore_extents ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${SERVER_BUILD_DIR}/libservices/libmysqlservices.a threadpool)

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_extents PROPERTIES PREFIX "")
set_target_properties(is_columnstore_extents PROPERTIES VERSION 1.0.0 SOVERSION 1)

SET ( is_columnstore_files_SRCS
    is_columnstore_files.cpp
    sm.cpp
    )
add_library(is_columnstore_files SHARED ${is_columnstore_files_SRCS})

target_link_libraries(is_columnstore_files ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${SERVER_BUILD_DIR}/libservices/libmysqlservices.a threadpool)

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_files PROPERTIES PREFIX "")
set_target_properties(is_columnstore_files PROPERTIES VERSION 1.0.0 SOVERSION 1)


install(TARGETS ha_columnstore is_columnstore_tables is_columnstore_columns is_columnstore_extents is_columnstore_files DESTINATION ${MARIADB_PLUGINDIR} COMPONENT storage-engine)
install(FILES syscatalog_mysql.sql 
              dumpcat_mysql.sql 
              calsetuserpriority.sql 
              calremoveuserpriority.sql 
              calshowprocesslist.sql 
              columnstore_info.sql
              DESTINATION ${ENGINE_SUPPORTDIR} COMPONENT storage-engine)
install(PROGRAMS install_mcs_mysql.sh mysql-Columnstore
    DESTINATION ${ENGINE_SBINDIR} COMPONENT storage-engine)

install(FILES columnstore.cnf
    DESTINATION ${MARIADB_MYCNFDIR} COMPONENT storage-engine)
