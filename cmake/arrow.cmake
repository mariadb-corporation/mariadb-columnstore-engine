if(NOT DEFINED WITH_PARQUET)
    set(WITH_PARQUET
        "auto"
        CACHE STRING "Which Parquet to use (possible values are 'off', 'bundled', 'system', or 'auto')"
    )
endif()

set(WITH_ARROW
    "auto"
    CACHE STRING "Which Arrow to use (possible values are 'off', 'bundled', 'system', or 'auto')"
)

set(ARROW_ENABLED OFF)
set(ARROW_PROVIDER "none")
set(ARROW_INCLUDE_DIRS "")
set(ARROW_LIBRARIES "")
set(ARROW_EXTERNAL_TARGET "")

set(PARQUET_BUNDLED_AVAILABLE OFF)
set(PARQUET_BUNDLED_LIBRARY_TARGET "")
set(PARQUET_BUNDLED_INCLUDE_DIRS "")

if(WITH_ARROW STREQUAL "off")
    return()
endif()

if(WITH_ARROW STREQUAL "system" OR WITH_ARROW STREQUAL "auto")
    find_package(Arrow CONFIG QUIET)

    if(TARGET Arrow::arrow_shared)
        set(ARROW_ENABLED ON)
        set(ARROW_PROVIDER "system")
        set(ARROW_LIBRARIES Arrow::arrow_shared)
    elseif(TARGET Arrow::arrow_static)
        set(ARROW_ENABLED ON)
        set(ARROW_PROVIDER "system")
        set(ARROW_LIBRARIES Arrow::arrow_static)
    endif()

    if(ARROW_ENABLED)
        add_custom_target(external_arrow)
        set(ARROW_EXTERNAL_TARGET external_arrow)
        if(DEFINED ARROW_INCLUDE_DIR)
            set(ARROW_INCLUDE_DIRS ${ARROW_INCLUDE_DIR})
        endif()
        return()
    elseif(WITH_ARROW STREQUAL "system")
        message(FATAL_ERROR "System Arrow requested but not found!")
    endif()
endif()

include(ExternalProject)

set(_arrow_enable_parquet OFF)
if(NOT WITH_PARQUET STREQUAL "off")
    set(_arrow_enable_parquet ON)
endif()

set(ARROW_VERSION "20.0.0")
set(ARROW_INSTALL_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/external/arrow)
set(ARROW_INCLUDE_DIRS "${ARROW_INSTALL_LOCATION}/include")
set(ARROW_LIBRARY_DIRS "${ARROW_INSTALL_LOCATION}/lib")
set(ARROW_LIBRARY "${ARROW_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}arrow${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(ARROW_BUNDLED_DEPS_LIBRARY
    "${ARROW_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX}"
)
set(PARQUET_LIBRARY "${ARROW_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}parquet${CMAKE_STATIC_LIBRARY_SUFFIX}")

ExternalProject_Add(
    external_arrow
    URL https://github.com/apache/arrow/archive/refs/tags/apache-arrow-20.0.0.tar.gz
    URL_HASH SHA256=67e31a4f46528634b8c3cbb0dc60ac8f85859d906b400d83d0b6f732b0c5b0e3
    PREFIX ${ARROW_INSTALL_LOCATION}
    SOURCE_SUBDIR cpp
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${ARROW_INSTALL_LOCATION}
               -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
               -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
               -DCMAKE_POSITION_INDEPENDENT_CODE=ON
               -DARROW_BUILD_SHARED=OFF
               -DARROW_BUILD_STATIC=ON
               -DARROW_PARQUET=${_arrow_enable_parquet}
               -DARROW_FILESYSTEM=OFF
               -DARROW_DATASET=OFF
               -DARROW_ACERO=OFF
               -DARROW_COMPUTE=OFF
               -DARROW_CSV=OFF
               -DARROW_JSON=OFF
               -DARROW_IPC=OFF
               -DARROW_BUILD_UTILITIES=OFF
               -DARROW_BUILD_TESTS=OFF
               -DARROW_BUILD_BENCHMARKS=OFF
               -DPARQUET_BUILD_EXECUTABLES=OFF
               -DPARQUET_BUILD_EXAMPLES=OFF
               -DPARQUET_BUILD_TESTS=OFF
               -DARROW_DEPENDENCY_SOURCE=BUNDLED
               -DCMAKE_INSTALL_MESSAGE=NEVER
    BUILD_BYPRODUCTS ${ARROW_LIBRARY} ${ARROW_BUNDLED_DEPS_LIBRARY} ${PARQUET_LIBRARY}
    EXCLUDE_FROM_ALL TRUE
)

add_library(CS_BUNDLED_ARROW_STATIC STATIC IMPORTED GLOBAL)
add_dependencies(CS_BUNDLED_ARROW_STATIC external_arrow)
set_target_properties(CS_BUNDLED_ARROW_STATIC PROPERTIES IMPORTED_LOCATION ${ARROW_LIBRARY})
add_library(CS_BUNDLED_ARROW_BUNDLED_DEPS_STATIC STATIC IMPORTED GLOBAL)
add_dependencies(CS_BUNDLED_ARROW_BUNDLED_DEPS_STATIC external_arrow)
set_target_properties(CS_BUNDLED_ARROW_BUNDLED_DEPS_STATIC PROPERTIES IMPORTED_LOCATION ${ARROW_BUNDLED_DEPS_LIBRARY})

set(ARROW_ENABLED ON)
set(ARROW_PROVIDER "bundled")
set(ARROW_LIBRARIES CS_BUNDLED_ARROW_STATIC CS_BUNDLED_ARROW_BUNDLED_DEPS_STATIC)
set(ARROW_EXTERNAL_TARGET external_arrow)

if(_arrow_enable_parquet)
    add_library(CS_BUNDLED_PARQUET_STATIC STATIC IMPORTED GLOBAL)
    add_dependencies(CS_BUNDLED_PARQUET_STATIC external_arrow)
    set_target_properties(CS_BUNDLED_PARQUET_STATIC PROPERTIES IMPORTED_LOCATION ${PARQUET_LIBRARY})

    set(PARQUET_BUNDLED_AVAILABLE ON)
    set(PARQUET_BUNDLED_LIBRARY_TARGET CS_BUNDLED_PARQUET_STATIC)
    set(PARQUET_BUNDLED_INCLUDE_DIRS ${ARROW_INCLUDE_DIRS})
endif()
