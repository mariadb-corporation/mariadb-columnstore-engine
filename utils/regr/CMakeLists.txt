
# regr
cs_add_library(regr SHARED 
  regr_avgx.cpp 
  regr_avgy.cpp 
  regr_count.cpp 
  regr_slope.cpp 
  regr_intercept.cpp 
  regr_r2.cpp 
  corr.cpp 
  regr_sxx.cpp 
  regr_syy.cpp 
  regr_sxy.cpp 
  covar_pop.cpp 
  covar_samp.cpp)

target_compile_definitions(regr PRIVATE MYSQL_DYNAMIC_PLUGIN)

set_target_properties(regr PROPERTIES VERSION 1.1.0 SOVERSION 1)

target_link_libraries(regr 
  PUBLIC 
  PRIVATE
    udfsdk_h 
    common_h 
    execplan_h
    messageqcpp_h
    loggingcpp_h
    mariadb
    funcexp_h
    joblist_h
    dataconvert_h)


install(TARGETS regr DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libs)




# regr_mysql

cs_add_library(regr_mysql SHARED regrmysql.cpp)

set_target_properties(regr_mysql PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_link_libraries(regr_mysql 
  PUBLIC 
  PRIVATE
    calmysql_h 
    mariadb 
    mariadb_sql 
    mariadb_pcre
    )

install(TARGETS regr_mysql DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT storage-engine)

