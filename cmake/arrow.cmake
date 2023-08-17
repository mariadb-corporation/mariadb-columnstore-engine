#[[include(ExternalProject)

set(INSTALL_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/external/arrow)
SET(ARROW_INCLUDE_DIR "${INSTALL_LOCATION}/include")
SET(ARROW_LIBRARY_DIR "${INSTALL_LOCATION}/lib")
LINK_DIRECTORIES("${ARROW_LIBRARY_DIR}")

ExternalProject_Add(external_arrow
    URL https://github.com/apache/arrow/archive/refs/tags/apache-arrow-12.0.1.tar.gz
    URL_HASH SHA256=3481c411393aa15c75e88d93cf8315faf7f43e180fe0790128d3840d417de858
    PREFIX ${INSTALL_LOCATION}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${INSTALL_LOCATION}
    -DARROW_COMPUTE=ON
    -DARROW_DATASET=ON
    -DARROW_PARQUET=ON
    -DARROW_FILESYSTEM=ON
    -DCMAKE_CXX_FLAGS:STRING="-fPIC"
    BUILD_BYPRODUCTS "${ARROW_LIBRARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}arrow${CMAKE_STATIC_LIBRARY_SUFFIX}"
    BUILD_BYPRODUCTS "${ARROW_LIBRARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}parquet${CMAKE_STATIC_LIBRARY_SUFFIX}"
    EXCLUDE_FROM_ALL TRUE
)

add_dependencies(external_arrow external_thrift)]]

# build_arrow function, with some modifications, is taken from:
# https://stackoverflow.com/questions/73935448/installing-and-linking-to-apache-arrow-inside-cmake
# Build the Arrow C++ libraries.
function(build_arrow)
    # If Arrow needs to be built, the default location will be within the build tree.
    set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/external/arrow")

    set(ARROW_SHARED_LIBRARY_DIR "${ARROW_PREFIX}/lib")

    set(ARROW_SHARED_LIB_FILENAME
            "${CMAKE_SHARED_LIBRARY_PREFIX}arrow${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(ARROW_SHARED_LIB "${ARROW_SHARED_LIBRARY_DIR}/${ARROW_SHARED_LIB_FILENAME}")
    set(PARQUET_SHARED_LIB_FILENAME
            "${CMAKE_SHARED_LIBRARY_PREFIX}parquet${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(PARQUET_SHARED_LIB "${ARROW_SHARED_LIBRARY_DIR}/${PARQUET_SHARED_LIB_FILENAME}")

    set(ARROW_BINARY_DIR "${ARROW_PREFIX}/bin")
    set(ARROW_CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}"
            "-DCMAKE_INSTALL_LIBDIR=lib"
            "-DARROW_BUILD_STATIC=OFF"
	    "-DARROW_DATASET=ON"
	    "-DARROW_PARQUET=ON"
	    "-DARROW_FILESYSTEM=ON")
    set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")

    set(ARROW_BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}" "${PARQUET_SHARED_LIB}")

    include(ExternalProject)

    externalproject_add(external_arrow
            URL https://github.com/apache/arrow/archive/refs/tags/apache-arrow-12.0.1.tar.gz
	    URL_HASH SHA256=f01b76a42ceb30409e7b1953ef64379297dd0c08502547cae6aaafd2c4a4d92e
	    SOURCE_SUBDIR cpp
	    BINARY_DIR "${ARROW_BINARY_DIR}"
            CMAKE_ARGS "${ARROW_CMAKE_ARGS}"
            BUILD_BYPRODUCTS "${ARROW_BUILD_BYPRODUCTS}")

    set(ARROW_LIBRARY_TARGET arrow_shared)
    set(PARQUET_LIBRARY_TARGET parquet_shared)

    file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")
    add_library(${ARROW_LIBRARY_TARGET} SHARED IMPORTED)
    add_library(${PARQUET_LIBRARY_TARGET} SHARED IMPORTED)
    set_target_properties(${ARROW_LIBRARY_TARGET}
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR}
            IMPORTED_LOCATION ${ARROW_SHARED_LIB})
    set_target_properties(${PARQUET_LIBRARY_TARGET}
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR}
            IMPORTED_LOCATION ${PARQUET_SHARED_LIB})

    add_dependencies(external_arrow external_thrift)
    add_dependencies(${ARROW_LIBRARY_TARGET} external_arrow)
endfunction()
build_arrow()
