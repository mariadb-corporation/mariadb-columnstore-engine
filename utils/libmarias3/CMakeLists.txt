set(S3API_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libmarias3 CACHE INTERNAL "S3API_DIR")

find_package(Git QUIET)

if(GIT_FOUND AND EXISTS ${ENGINE_SRC_DIR}/.git)
# Update submodules as needed
    option(GIT_SUBMODULE "Check submodules during build" ON)
    if(GIT_SUBMODULE)
        message(STATUS "Submodule update")
        execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
            WORKING_DIRECTORY ${ENGINE_SRC_DIR}
                        RESULT_VARIABLE GIT_SUBMOD_RESULT)
        if(NOT GIT_SUBMOD_RESULT EQUAL "0")
            message(FATAL_ERROR "git submodule update --init failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
        endif()
    endif()
endif()

SET(S3_SOURCES ${S3API_DIR}/src/debug.c
    ${S3API_DIR}/src/error.c
    ${S3API_DIR}/src/marias3.c
    ${S3API_DIR}/src/request.c
    ${S3API_DIR}/src/response.c
    ${S3API_DIR}/src/sha256.c
    ${S3API_DIR}/src/sha256-internal.c
    ${S3API_DIR}/src/xml.c)

ADD_LIBRARY(marias3 SHARED ${S3_SOURCES})

FIND_PACKAGE(CURL REQUIRED)
TARGET_LINK_LIBRARIES(marias3 curl)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR} ${S3API_DIR})

set(S3API_DEPS marias3 curl CACHE INTERNAL "S3API_DEPS")

install(TARGETS marias3
    DESTINATION ${ENGINE_LIBDIR}
    COMPONENT columnstore-platform
)
