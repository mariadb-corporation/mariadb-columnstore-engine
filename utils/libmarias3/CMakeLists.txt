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

include(ExternalProject)
ExternalProject_Add(ms3
    SOURCE_DIR ${S3API_DIR}
    CONFIGURE_COMMAND autoreconf -fi ${S3API_DIR} && ${S3API_DIR}/configure --enable-shared --disable-static --prefix=${CMAKE_BINARY_DIR} ${S3_CONFIGURE_OPT}
    BUILD_COMMAND make
    BUILD_IN_SOURCE 0
    EXCLUDE_FROM_ALL TRUE
    BUILD_BYPRODUCTS ${CMAKE_BINARY_DIR}/lib/libmarias3.so.3.1.2
)

set(S3API_DEPS marias3 curl xml2 CACHE INTERNAL "S3API_DEPS")

install(PROGRAMS
    ${CMAKE_BINARY_DIR}/lib/libmarias3.so.3.1.2
    ${CMAKE_BINARY_DIR}/lib/libmarias3.so.3
    ${CMAKE_BINARY_DIR}/lib/libmarias3.so
    DESTINATION ${ENGINE_LIBDIR}
    COMPONENT columnstore-platform
)
