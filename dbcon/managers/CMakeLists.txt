include_directories( ${ENGINE_COMMON_INCLUDES} ${ENGINE_SRC_DIR}/tools/passwd)
########### next target ###############

set(managers_LIB_SRCS
    resourcemanager.cpp
    resourcedistributor.cpp
    jl_logger.cpp
    ${ENGINE_SRC_DIR}/tools/passwd/secrets.cpp)
########### next target ###############

add_definitions(-DMYSQL_SERVICE_THD_TIMEZONE_INCLUDED)

add_library(managers STATIC ${managers_LIB_SRCS})

target_include_directories(managers BEFORE PUBLIC ${OPENSSL_INCLUDE_DIR}
  ${LIBMARIADB_BININC_DIR}
  ${LIBMARIADB_SRCINC_DIR})
add_dependencies(managers loggingcpp)

target_link_libraries(managers common loggingcpp )

install(TARGETS managers DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)


