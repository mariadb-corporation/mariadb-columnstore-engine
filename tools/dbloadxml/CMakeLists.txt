include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(dbload_STAT_SRCS inputmgr.cpp)

add_library(dbload STATIC ${dbload_STAT_SRCS})

add_dependencies(dbload loggingcpp)

# ########## next target ###############

set(colxml_SRCS colxml.cpp)

columnstore_executable(colxml ${colxml_SRCS})

columnstore_link(colxml ${ENGINE_LDFLAGS} dbload ${ENGINE_WRITE_LIBS})
