configure_file("${CMAKE_CURRENT_SOURCE_DIR}/install_mcs_mysql.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/install_mcs_mysql.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/ha_mcs_version.h.in" "${CMAKE_CURRENT_SOURCE_DIR}/ha_mcs_version.h")

include_directories( ${ENGINE_COMMON_INCLUDES}
                     /usr/include/libxml2
                     ${SERVER_SOURCE_ROOT_DIR}/storage/maria )


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
    ha_pseudocolumn.cpp
    is_columnstore_tables.cpp
    is_columnstore_columns.cpp
    is_columnstore_files.cpp
    is_columnstore_extents.cpp)

add_definitions(-DMYSQL_DYNAMIC_PLUGIN)

set_source_files_properties(ha_mcs.cpp PROPERTIES COMPILE_FLAGS "-fno-implicit-templates")

if (COMMAND mysql_add_plugin)
  IF(NOT(RPM OR DEB))
    SET(disabled DISABLED)
  ENDIF()
  
  FILE(COPY columnstore.cnf DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/)
  if (BUILD_CONFIG STREQUAL "enterprise")
     FILE(RENAME ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/columnstore.cnf ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/x-columnstore.cnf)
     SET(CS_CNF_FILE "${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/x-columnstore.cnf")
     install(FILES columnstore.cnf DESTINATION ${MARIADB_MYCNFDIR} COMPONENT columnstore-engine)
   else ()
     SET(CS_CNF_FILE "${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/columnstore.cnf")
   endif ()

  mysql_add_plugin(columnstore ${libcalmysql_SRCS} STORAGE_ENGINE MODULE_ONLY ${disabled}
      LINK_LIBRARIES ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${NETSNMP_LIBRARIES} threadpool
      COMPONENT columnstore-engine CONFIG ${CS_CNF_FILE})
else ()
    add_library(ha_columnstore SHARED ${libcalmysql_SRCS})
    SET_TARGET_PROPERTIES(ha_columnstore PROPERTIES PREFIX "")

    add_dependencies(ha_columnstore loggingcpp)
    
    target_link_libraries(ha_columnstore ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${NETSNMP_LIBRARIES} ${SERVER_BUILD_DIR}/libservices/libmysqlservices.a threadpool)

    install(TARGETS ha_columnstore DESTINATION ${MARIADB_PLUGINDIR} COMPONENT columnstore-engine)
    
    # define this dummy target for standalone builds (ie, when mysql_add_plugin doesn't exist)
    add_custom_target(columnstore DEPENDS ha_columnstore) 

    install(FILES columnstore.cnf DESTINATION ${MARIADB_MYCNFDIR} COMPONENT columnstore-engine)
endif ()

if (TARGET columnstore)
    install(FILES syscatalog_mysql.sql 
                  dumpcat_mysql.sql 
                  calsetuserpriority.sql 
                  calremoveuserpriority.sql 
                  calshowprocesslist.sql 
                  columnstore_info.sql
                  DESTINATION ${ENGINE_SUPPORTDIR} COMPONENT columnstore-engine)
    install(PROGRAMS install_mcs_mysql.sh
        DESTINATION ${ENGINE_SBINDIR} COMPONENT columnstore-engine)
endif()
