
target_include_directories(calmysql_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

# calmysql

cs_add_library(calmysql SHARED 
    ha_mcs_sysvars.cpp
    ha_mcs_client_udfs.cpp
    ha_calpont.cpp
    ha_calpont_impl.cpp
    ha_calpont_dml.cpp
    ha_calpont_ddl.cpp
    ha_calpont_execplan.cpp
    ha_scalar_sub.cpp
    ha_in_sub.cpp
    ha_exists_sub.cpp
    ha_from_sub.cpp
    ha_select_sub.cpp
    ha_view.cpp 
    sm.cpp
    ha_window_function.cpp
    ha_calpont_partition.cpp
    ha_pseudocolumn.cpp)

target_compile_definitions(calmysql PRIVATE MYSQL_DYNAMIC_PLUGIN)

#set_source_files_properties(ha_calpont.cpp PROPERTIES COMPILE_FLAGS "-fno-rtti -fno-implicit-templates")

target_link_libraries(calmysql 
  PRIVATE calmysql_h
          alarmmanager_h
          brm_h
          cacheutils_h
          columnstore_h
          common_h
          compress_h
          configcpp_h
          dataconvert_h
          ddlpackage_h
          ddlpackageproc_h
          dmlpackage_h
          dmlpackageproc_h
          execplan_h 
          funcexp_h
          idbboot_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          mariadb
          mariadb_pcre 
          mariadb_sql 
          messageqcpp_h 
          oamcpp_h
          querystats_h
          querytele_h
          rowgroup_h
          rwlock_h
          udfsdk_h
          writeengineclient_h
          we_shared_h
          writeengine_h)

set_target_properties(calmysql PROPERTIES VERSION 1.0.0 SOVERSION 1)



# is_columnstore_tables

cs_add_library(is_columnstore_tables SHARED is_columnstore_tables.cpp sm.cpp)

target_compile_definitions(is_columnstore_tables PRIVATE MYSQL_DYNAMIC_PLUGIN)

target_link_libraries(is_columnstore_tables 
  PRIVATE mariadb 
          mariadb_sql 
          mariadb_pcre 
          messageqcpp_h 
          querystats_h
          execplan_h
          loggingcpp_h
          configcpp_h
          brm_h
          idbboot_h
          common_h
          joblist_h
          rowgroup_h
          dataconvert_h
          udfsdk_h)

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_tables PROPERTIES PREFIX "")
set_target_properties(is_columnstore_tables PROPERTIES VERSION 1.0.0 SOVERSION 1)


# is_columnstore_columns

cs_add_library(is_columnstore_columns SHARED 
    is_columnstore_columns.cpp
    sm.cpp
    )

target_compile_definitions(is_columnstore_columns PRIVATE MYSQL_DYNAMIC_PLUGIN)

target_link_libraries(is_columnstore_columns 
  PRIVATE mariadb 
          mariadb_sql 
          mariadb_pcre 
          messageqcpp_h 
          execplan_h 
          querystats_h
          joblist_h
          dataconvert_h
          loggingcpp_h
          configcpp_h
          brm_h
          idbboot_h
          common_h
          rowgroup_h
          udfsdk_h)

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_columns PROPERTIES PREFIX "")
set_target_properties(is_columnstore_columns PROPERTIES VERSION 1.0.0 SOVERSION 1)

# is_columnstore_extents

cs_add_library(is_columnstore_extents SHARED 
    is_columnstore_extents.cpp
    sm.cpp
    )

target_compile_definitions(is_columnstore_extents PRIVATE MYSQL_DYNAMIC_PLUGIN)

target_link_libraries(is_columnstore_extents 
  PRIVATE brm_h
        common_h
        configcpp_h
        dataconvert_h
        execplan_h
        idbboot_h
        joblist_h
        loggingcpp_h
        mariadb
        mariadb_pcre
        mariadb_sql
        messageqcpp_h 
        oamcpp_h
        querystats_h
        rowgroup_h
        rwlock_h
        udfsdk_h)

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_extents PROPERTIES PREFIX "")
set_target_properties(is_columnstore_extents PROPERTIES VERSION 1.0.0 SOVERSION 1)


# is_columnstore_files

cs_add_library(is_columnstore_files SHARED 
    is_columnstore_files.cpp
    sm.cpp
    )

target_compile_definitions(is_columnstore_files PRIVATE MYSQL_DYNAMIC_PLUGIN)

target_link_libraries(is_columnstore_files 
  PRIVATE alarmmanager_h
        brm_h
        common_h
        configcpp_h
        dataconvert_h
        execplan_h
        idbboot_h
        idbdatafile_h
        joblist_h
        loggingcpp_h
        mariadb 
        mariadb_pcre
        mariadb_sql 
        messageqcpp_h 
        oamcpp_h
        querystats_h
        rowgroup_h
        rwlock_h
        udfsdk_h
        we_server_h
        we_shared_h
          )

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_files PROPERTIES PREFIX "")
set_target_properties(is_columnstore_files PROPERTIES VERSION 1.0.0 SOVERSION 1)


install(TARGETS calmysql 
                is_columnstore_tables 
                is_columnstore_columns 
                is_columnstore_extents 
                is_columnstore_files 
        DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT storage-engine)

install(FILES syscatalog_mysql.sql 
              dumpcat_mysql.sql 
              calsetuserpriority.sql 
              calremoveuserpriority.sql 
              calshowprocesslist.sql 
              columnstore_info.sql
              my.cnf
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR} COMPONENT storage-engine)

install(PROGRAMS install_calpont_mysql.sh mysql-Columnstore
        DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT storage-engine)
