set(S3API_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libmarias3 CACHE INTERNAL "S3API_DIR")

include(ExternalProject)
ExternalProject_Add(ms3
    DOWNLOAD_COMMAND cd ${CMAKE_SOURCE_DIR} && git submodule update --init
    CONFIGURE_COMMAND autoreconf -fi ${S3API_DIR} && ${S3API_DIR}/configure --enable-shared --disable-static --prefix=${CMAKE_BINARY_DIR} ${S3_CONFIGURE_OPT}
    BUILD_COMMAND make
    BUILD_IN_SOURCE 0
    EXCLUDE_FROM_ALL TRUE
)

add_library(marias3 SHARED IMPORTED)
set_target_properties(marias3 PROPERTIES IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libmarias3.so.3.1.2)
add_dependencies(marias3 ms3)

set(S3API_DEPS marias3 curl xml2 CACHE INTERNAL "S3API_DEPS")

install(PROGRAMS
    ${CMAKE_BINARY_DIR}/lib/libmarias3.so.3.1.2
    ${CMAKE_BINARY_DIR}/lib/libmarias3.so.3
    ${CMAKE_BINARY_DIR}/lib/libmarias3.so
    DESTINATION ${INSTALL_ENGINE}/lib
    COMPONENT platform
)
