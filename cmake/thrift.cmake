set(WITH_THRIFT
    "bundled"
    CACHE STRING "Which Thrift to use (possible values are 'bundled', 'system', or 'auto')"
)

if(WITH_THRIFT STREQUAL "system" OR WITH_THRIFT STREQUAL "auto")
    find_package(Thrift)

    if(Thrift_FOUND)
        add_custom_target(external_thrift)
        set(THRIFT_INCLUDE_DIR "${THRIFT_INCLUDE_DIR}")
        set(THRIFT_LIBRARY "${THRIFT_LIBRARIES}")
        return()
    elseif(WITH_THRIFT STREQUAL "system")
        message(FATAL_ERROR "System Thrift requested but not found!")
    endif()
endif()

include(ExternalProject)

set(INSTALL_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/external/thrift)
set(THRIFT_INCLUDE_DIRS "${INSTALL_LOCATION}/include")
set(THRIFT_LIBRARY_DIRS "${INSTALL_LOCATION}/lib")
set(THRIFT_LIBRARY ${THRIFT_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}thrift${CMAKE_STATIC_LIBRARY_SUFFIX})

ExternalProject_Add(
    external_thrift
    URL https://github.com/apache/thrift/archive/refs/tags/v0.17.0.tar.gz
    URL_HASH SHA256=f5888bcd3b8de40c2c2ab86896867ad9b18510deb412cba3e5da76fb4c604c29
    PREFIX ${INSTALL_LOCATION}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${INSTALL_LOCATION}
               -DBUILD_COMPILER=YES
               -DBUILD_CPP=YES
               -DBUILD_C_GLIB=YES
               -DBUILD_JAVA=NO
               -DBUILD_JAVASCRIPT=NO
               -DBUILD_KOTLIN=NO
               -DBUILD_NODEJS=NO
               -DBUILD_PYTHON=NO
               -DBUILD_TESTING=NO
               -DBUILD_SHARED_LIBS=NO
               -DCMAKE_CXX_FLAGS:STRING="-fPIC"
               -DBOOST_ROOT=${BOOST_ROOT}
    BUILD_BYPRODUCTS "${THRIFT_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}thrift${CMAKE_STATIC_LIBRARY_SUFFIX}"
    EXCLUDE_FROM_ALL TRUE
)

add_dependencies(external_thrift external_boost)
