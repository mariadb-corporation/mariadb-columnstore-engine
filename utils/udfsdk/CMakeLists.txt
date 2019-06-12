
target_include_directories(udfsdk_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


# udfsdk
cs_add_library(udfsdk STATIC
  udfsdk.cpp 
  mcsv1_udaf.cpp 
  allnull.cpp 
  ssq.cpp 
  avg_mode.cpp 
  avgx.cpp 
  distinct_count.cpp
  )

target_link_libraries(udfsdk
  INTERFACE udfsdk_h
  PUBLIC
  PRIVATE
    funcexp_h
    common_h
    rowgroup_h
    execplan_h
    messageqcpp_h
    joblist_h
    loggingcpp_h
    dataconvert_h
    mariadb
          )

target_compile_definitions(udfsdk PRIVATE MYSQL_DYNAMIC_PLUGIN)

set_target_properties(udfsdk PROPERTIES VERSION 1.1.0 SOVERSION 1)


install(TARGETS udfsdk DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libs)


# udf_mysql

cs_add_library(udf_mysql SHARED udfmysql.cpp)

set_target_properties(udf_mysql PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_link_libraries(udf_mysql 
  PRIVATE 
    calmysql_h 
    mariadb 
    mariadb_sql 
    mariadb_pcre
  )

install(TARGETS udf_mysql DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT storage-engine)

