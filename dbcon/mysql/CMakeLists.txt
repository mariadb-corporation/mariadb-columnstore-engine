
include_directories( ${ENGINE_COMMON_INCLUDES}
                     /usr/include/libxml2 )


SET ( libcalmysql_SRCS
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
    ha_view.cpp sm.cpp
    ha_window_function.cpp
    ha_calpont_partition.cpp
    ha_pseudocolumn.cpp)

add_definitions(-DMYSQL_DYNAMIC_PLUGIN)

set_source_files_properties(ha_calpont.cpp PROPERTIES COMPILE_FLAGS "-fno-rtti -fno-implicit-templates")

add_library(calmysql SHARED ${libcalmysql_SRCS})

target_link_libraries(calmysql ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${NETSNMP_LIBRARIES} ${SERVER_BUILD_INCLUDE_DIR}/../libservices/libmysqlservices.a threadpool)

set_target_properties(calmysql PROPERTIES VERSION 1.0.0 SOVERSION 1)

SET ( is_columnstore_tables_SRCS
    is_columnstore_tables.cpp
    )
add_library(is_columnstore_tables SHARED ${is_columnstore_tables_SRCS})

target_link_libraries(is_columnstore_tables ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${SERVER_BUILD_INCLUDE_DIR}/../libservices/libmysqlservices.a threadpool)

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_tables PROPERTIES PREFIX "")
set_target_properties(is_columnstore_tables PROPERTIES VERSION 1.0.0 SOVERSION 1)

SET ( is_columnstore_columns_SRCS
    is_columnstore_columns.cpp
    )
add_library(is_columnstore_columns SHARED ${is_columnstore_columns_SRCS})

target_link_libraries(is_columnstore_columns ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${SERVER_BUILD_INCLUDE_DIR}/../libservices/libmysqlservices.a threadpool)

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_columns PROPERTIES PREFIX "")
set_target_properties(is_columnstore_columns PROPERTIES VERSION 1.0.0 SOVERSION 1)

SET ( is_columnstore_extents_SRCS
    is_columnstore_extents.cpp
    )
add_library(is_columnstore_extents SHARED ${is_columnstore_extents_SRCS})

target_link_libraries(is_columnstore_extents ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${SERVER_BUILD_INCLUDE_DIR}/../libservices/libmysqlservices.a threadpool)

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_extents PROPERTIES PREFIX "")
set_target_properties(is_columnstore_extents PROPERTIES VERSION 1.0.0 SOVERSION 1)

SET ( is_columnstore_files_SRCS
    is_columnstore_files.cpp
    )
add_library(is_columnstore_files SHARED ${is_columnstore_files_SRCS})

target_link_libraries(is_columnstore_files ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${SERVER_BUILD_INCLUDE_DIR}/../libservices/libmysqlservices.a threadpool)

# Don't prepend .so file with 'lib'
set_target_properties(is_columnstore_files PROPERTIES PREFIX "")
set_target_properties(is_columnstore_files PROPERTIES VERSION 1.0.0 SOVERSION 1)


install(TARGETS calmysql is_columnstore_tables is_columnstore_columns is_columnstore_extents is_columnstore_files DESTINATION ${ENGINE_LIBDIR} COMPONENT storage-engine)
install(FILES syscatalog_mysql.sql 
              dumpcat_mysql.sql 
              calsetuserpriority.sql 
              calremoveuserpriority.sql 
              calshowprocesslist.sql 
              columnstore_info.sql
              my.cnf
              DESTINATION ${ENGINE_MYSQLDIR} COMPONENT storage-engine)
install(PROGRAMS install_calpont_mysql.sh mysql-Columnstore
              DESTINATION ${ENGINE_MYSQLDIR} COMPONENT storage-engine)


#AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
#AM_CFLAGS = $(idb_cflags)
#AM_CXXFLAGS = $(idb_cxxflags)
#AM_LDFLAGS = $(idb_ldflags)
#lib_LTLIBRARIES = libcalmysql.la
#libcalmysql_la_SOURCES = ha_calpont.cpp ha_calpont_impl.cpp ha_calpont_dml.cpp ha_calpont_ddl.cpp ha_calpont_execplan.cpp ha_scalar_sub.cpp ha_in_sub.cpp ha_exists_sub.cpp ha_from_sub.cpp ha_select_sub.cpp ha_view.cpp sm.cpp ha_window_function.cpp ha_calpont_partition.cpp ha_pseudocolumn.cpp
#libcalmysql_la_LDFLAGS = -version-info 1:0:0 $(idb_common_ldflags) $(idb_common_libs) $(idb_write_libs) $(AM_LDFLAGS)
#libcalmysql_la_CPPFLAGS = -I/usr/include/libxml2 -I../../../mysql/include -I../../../mysql/sql -I../../../mysql/regex -DMYSQL_DYNAMIC_PLUGIN $(AM_CPPFLAGS)
#include_HEADERS = idb_mysql.h
#
#dist_mysql_DATA = syscatalog_mysql.sql dumpcat_mysql.sql calsetuserpriority.sql calremoveuserpriority.sql calshowprocesslist.sql my.cnf
#dist_mysql_SCRIPTS = install_calpont_mysql.sh mysql-Columnstore dumpcat.pl
#
#libcalmysql_la-ha_calpont.lo: ha_calpont.cpp
#    if $(LIBTOOL) --tag=CXX --mode=compile $(CXX) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) $(libcalmysql_la_CPPFLAGS) $(CPPFLAGS) $(AM_CXXFLAGS) $(CXXFLAGS) -fno-rtti -fno-implicit-templates -MT libcalmysql_la-ha_calpont.lo -MD -MP -MF "$(DEPDIR)/libcalmysql_la-ha_calpont.Tpo" -c -o libcalmysql_la-ha_calpont.lo `test -f 'ha_calpont.cpp' || echo '$(srcdir)/'`ha_calpont.cpp; \
#    then mv -f "$(DEPDIR)/libcalmysql_la-ha_calpont.Tpo" "$(DEPDIR)/libcalmysql_la-ha_calpont.Plo"; else rm -f "$(DEPDIR)/libcalmysql_la-ha_calpont.Tpo"; exit 1; fi

