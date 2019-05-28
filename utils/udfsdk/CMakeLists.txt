
target_include_directories(udfsdk_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


# udfsdk
add_library(udfsdk SHARED 
  udfsdk.cpp 
  mcsv1_udaf.cpp 
  allnull.cpp 
  ssq.cpp 
  avg_mode.cpp 
  avgx.cpp 
  distinct_count.cpp)

target_link_libraries(udfsdk
  PUBLIC udfsdk_h
  PRIVATE mariadb
          dataconvert_h
          funcexp_h
          joblist_h
          execplan_h       
          rowgroup_h
          common_h
          messageqcpp_h
          loggingcpp_h)

target_compile_definitions(udfsdk PRIVATE MYSQL_DYNAMIC_PLUGIN)

set_target_properties(udfsdk PROPERTIES VERSION 1.1.0 SOVERSION 1)

target_pch(udfsdk)

install(TARGETS udfsdk DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


# udf_mysql

add_library(udf_mysql SHARED udfmysql.cpp)

set_target_properties(udf_mysql PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_link_libraries(udf_mysql 
  PRIVATE calmysql_h 
          mariadb 
          mariadb_sql 
          mariadb_pcre)

install(TARGETS udf_mysql DESTINATION ${ENGINE_LIBDIR} COMPONENT storage-engine)

