set(S3API_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/libmarias3
    CACHE INTERNAL "S3API_DIR"
)

set(S3_SOURCES
    ${S3API_DIR}/src/debug.c
    ${S3API_DIR}/src/error.c
    ${S3API_DIR}/src/marias3.c
    ${S3API_DIR}/src/request.c
    ${S3API_DIR}/src/response.c
    ${S3API_DIR}/src/sha256.c
    ${S3API_DIR}/src/sha256-internal.c
    ${S3API_DIR}/src/xml.c
    ${S3API_DIR}/src/assume_role.c
)

columnstore_shared_library(marias3 ${S3_SOURCES})
columnstore_link(marias3 curl m)
include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${S3API_DIR})

add_definitions(-D_GNU_SOURCE)

set(S3API_DEPS
    marias3 curl
    CACHE INTERNAL "S3API_DEPS"
)

install(
    TARGETS marias3
    DESTINATION ${ENGINE_LIBDIR}
    COMPONENT columnstore-engine
)
