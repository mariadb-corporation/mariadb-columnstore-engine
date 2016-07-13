
include_directories( ${ENGINE_COMMON_INCLUDES} )


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
    ha_pseudocolumn.cpp )

add_definitions(-DMYSQL_DYNAMIC_PLUGIN)

add_library(calmysql SHARED ${libcalmysql_SRCS})

set_target_properties(calmysql PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS calmysql DESTINATION ${ENGINE_LIBDIR})
install(FILES syscatalog_mysql.sql 
              dumpcat_mysql.sql 
              calsetuserpriority.sql 
              calremoveuserpriority.sql 
              calshowprocesslist.sql 
              my.cnf
              DESTINATION ${ENGINE_MYSQLDIR})
install(FILES install_calpont_mysql.sh mysql-Columnstore dumpcat.pl
              DESTINATION ${ENGINE_MYSQLDIR})


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

