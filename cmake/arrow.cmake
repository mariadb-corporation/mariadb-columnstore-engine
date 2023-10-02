# This is the modified version of:
# https://stackoverflow.com/questions/73935448/installing-and-linking-to-apache-arrow-inside-cmake

# Build the Arrow C++ libraries.
include(ExternalProject)

set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/external/arrow")
set(ARROW_STATIC_LIBRARY_DIR "${ARROW_PREFIX}/lib")

set(ARROW_STATIC_LIB_FILENAME
    "${CMAKE_STATIC_LIBRARY_PREFIX}arrow${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(ARROW_STATIC_LIB "${ARROW_STATIC_LIBRARY_DIR}/${ARROW_STATIC_LIB_FILENAME}")

set(ARROW_DEPS_STATIC_LIB_FILENAME
    "${CMAKE_STATIC_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(ARROW_DEPS_STATIC_LIB "${ARROW_STATIC_LIBRARY_DIR}/${ARROW_DEPS_STATIC_LIB_FILENAME}")

set(PARQUET_STATIC_LIB_FILENAME
    "${CMAKE_STATIC_LIBRARY_PREFIX}parquet${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(PARQUET_STATIC_LIB "${ARROW_STATIC_LIBRARY_DIR}/${PARQUET_STATIC_LIB_FILENAME}")

link_directories("${ARROW_STATIC_LIBRARY_DIR}")

set(ARROW_BINARY_DIR "${ARROW_PREFIX}/bin")
set(ARROW_CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}"
        "-DCMAKE_INSTALL_LIBDIR=lib"
        "-DARROW_BUILD_STATIC=ON"
        "-DARROW_BUILD_SHARED=OFF"
        "-DARROW_DEPENDENCY_SOURCE=BUNDLED"
        "-DARROW_DEPENDENCY_USE_SHARED=OFF"
        "-DARROW_DATASET=ON"
        "-DARROW_PARQUET=ON"
        "-DARROW_FILESYSTEM=ON"
        "-DThrift_ROOT=${CMAKE_CURRENT_BINARY_DIR}/external/thrift"
        )
set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")
set(ARROW_BUILD_BYPRODUCTS "${ARROW_STATIC_LIB}" "${PARQUET_STATIC_LIB}")

externalproject_add(external_arrow
        URL https://github.com/apache/arrow/archive/refs/tags/go/v13.0.0.tar.gz
        URL_HASH SHA256=ea4a79a4103379573ecbcf19229437a4ba547c0146a7f3c3be0a7e0b3de5de6c
        SOURCE_SUBDIR cpp
        BINARY_DIR "${ARROW_BINARY_DIR}"
        CMAKE_ARGS "${ARROW_CMAKE_ARGS}"
        BUILD_BYPRODUCTS "${ARROW_BUILD_BYPRODUCTS}")


file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")

add_library(arrow STATIC IMPORTED)
add_library(parquet STATIC IMPORTED)
add_library(arrow_bundled_dependencies STATIC IMPORTED)

set_target_properties(arrow
        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR}
        IMPORTED_LOCATION ${ARROW_STATIC_LIB})

set_target_properties(arrow_bundled_dependencies
        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR}
        IMPORTED_LOCATION ${ARROW_DEPS_STATIC_LIB})

set_target_properties(parquet
        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR}
        IMPORTED_LOCATION ${PARQUET_STATIC_LIB})


target_link_libraries(arrow INTERFACE arrow_bundled_dependencies)
target_link_libraries(parquet INTERFACE arrow)

add_dependencies(external_arrow external_thrift)
add_dependencies(arrow external_arrow)
