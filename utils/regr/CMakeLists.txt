
include_directories( ${ENGINE_COMMON_INCLUDES}
                     ../../dbcon/mysql )
                     
########### next target ###############

set(regr_LIB_SRCS regr_avgx.cpp regr_avgy.cpp regr_count.cpp regr_slope.cpp regr_intercept.cpp regr_r2.cpp corr.cpp regr_sxx.cpp regr_syy.cpp regr_sxy.cpp covar_pop.cpp covar_samp.cpp moda.cpp)

add_definitions(-DMYSQL_DYNAMIC_PLUGIN)

add_library(regr SHARED ${regr_LIB_SRCS} )

add_dependencies(regr loggingcpp)

install(TARGETS regr DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)



set(regr_mysql_LIB_SRCS regrmysql.cpp modamysql.cpp)

add_library(regr_mysql SHARED ${regr_mysql_LIB_SRCS})

install(TARGETS regr_mysql DESTINATION ${MARIADB_PLUGINDIR} COMPONENT columnstore-engine)

set_target_properties(regr_mysql PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../../../)
