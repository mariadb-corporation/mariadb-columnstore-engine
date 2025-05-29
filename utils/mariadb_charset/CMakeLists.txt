include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

columnstore_static_library(mariadb_charset charset.cpp)
columnstore_link(mariadb_charset PRIVATE ${MARIADB_STRING_LIBS})
add_dependencies(mariadb_charset loggingcpp)
