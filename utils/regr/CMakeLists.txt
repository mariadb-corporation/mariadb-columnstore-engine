INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES} ../../dbcon/mysql)

# ########## next target ###############

SET (
  regr_LIB_SRCS
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
  covar_samp.cpp
  moda.cpp
)

ADD_DEFINITIONS (-DMYSQL_DYNAMIC_PLUGIN)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (regr STATIC ${regr_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (regr SHARED ${regr_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (regr loggingcpp)

INSTALL (
  TARGETS regr
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)

SET (regr_mysql_LIB_SRCS regrmysql.cpp modamysql.cpp)

ADD_LIBRARY (regr_mysql SHARED ${regr_mysql_LIB_SRCS})
ADD_DEPENDENCIES (regr_mysql external_boost)

INSTALL (
  TARGETS regr_mysql
  DESTINATION ${MARIADB_PLUGINDIR}
  COMPONENT columnstore-engine
)

SET_TARGET_PROPERTIES (regr_mysql PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../../../)
