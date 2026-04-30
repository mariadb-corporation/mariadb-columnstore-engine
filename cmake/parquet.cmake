set(WITH_PARQUET
    "auto"
    CACHE STRING "Which Parquet to use (possible values are 'off', 'bundled', 'system', or 'auto')"
)

set(PARQUET_ENABLED OFF)
set(PARQUET_INCLUDE_DIRS "")
set(PARQUET_LIBRARIES "")
set(PARQUET_EXTERNAL_TARGET "")

if(WITH_PARQUET STREQUAL "off")
    return()
endif()

if(NOT ARROW_ENABLED)
    message(FATAL_ERROR "Parquet requires Arrow. Enable WITH_ARROW first.")
endif()

if(WITH_PARQUET STREQUAL "system" OR WITH_PARQUET STREQUAL "auto")
    find_package(Parquet CONFIG QUIET)

    if(TARGET Parquet::parquet_shared)
        set(PARQUET_ENABLED ON)
        set(PARQUET_LIBRARIES Parquet::parquet_shared)
    elseif(TARGET Parquet::parquet_static)
        set(PARQUET_ENABLED ON)
        set(PARQUET_LIBRARIES Parquet::parquet_static)
    endif()

    if(PARQUET_ENABLED)
        add_custom_target(external_parquet)
        set(PARQUET_EXTERNAL_TARGET external_parquet)
        if(DEFINED PARQUET_INCLUDE_DIR)
            set(PARQUET_INCLUDE_DIRS ${PARQUET_INCLUDE_DIR})
        elseif(DEFINED ARROW_INCLUDE_DIR)
            set(PARQUET_INCLUDE_DIRS ${ARROW_INCLUDE_DIR})
        elseif(ARROW_INCLUDE_DIRS)
            set(PARQUET_INCLUDE_DIRS ${ARROW_INCLUDE_DIRS})
        endif()
        return()
    elseif(WITH_PARQUET STREQUAL "system")
        message(FATAL_ERROR "System Parquet requested but not found!")
    endif()
endif()

if(PARQUET_BUNDLED_AVAILABLE)
    add_custom_target(external_parquet DEPENDS ${ARROW_EXTERNAL_TARGET})
    set(PARQUET_EXTERNAL_TARGET external_parquet)
    set(PARQUET_ENABLED ON)
    set(PARQUET_INCLUDE_DIRS ${PARQUET_BUNDLED_INCLUDE_DIRS})
    set(PARQUET_LIBRARIES ${PARQUET_BUNDLED_LIBRARY_TARGET})
    return()
endif()

if(WITH_ARROW STREQUAL "system")
    message(FATAL_ERROR "Bundled Parquet fallback is incompatible with WITH_ARROW=system. Use WITH_ARROW=auto or WITH_ARROW=bundled.")
endif()

include(ExternalProject)

set(PARQUET_INSTALL_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/external/parquet)
set(PARQUET_INCLUDE_DIRS "${PARQUET_INSTALL_LOCATION}/include")
set(PARQUET_LIBRARY_DIRS "${PARQUET_INSTALL_LOCATION}/lib")
set(PARQUET_ARROW_LIBRARY "${PARQUET_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}arrow${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(PARQUET_LIBRARY "${PARQUET_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}parquet${CMAKE_STATIC_LIBRARY_SUFFIX}")

ExternalProject_Add(
    external_parquet
    URL https://github.com/apache/arrow/archive/refs/tags/apache-arrow-20.0.0.tar.gz
    URL_HASH SHA256=67e31a4f46528634b8c3cbb0dc60ac8f85859d906b400d83d0b6f732b0c5b0e3
    PREFIX ${PARQUET_INSTALL_LOCATION}
    SOURCE_SUBDIR cpp
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${PARQUET_INSTALL_LOCATION}
               -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
               -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
               -DCMAKE_POSITION_INDEPENDENT_CODE=ON
               -DARROW_BUILD_SHARED=OFF
               -DARROW_BUILD_STATIC=ON
               -DARROW_PARQUET=ON
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
    BUILD_BYPRODUCTS ${PARQUET_ARROW_LIBRARY} ${PARQUET_LIBRARY}
    EXCLUDE_FROM_ALL TRUE
)

add_library(CS_PARQUET_FALLBACK_ARROW_STATIC STATIC IMPORTED GLOBAL)
add_library(CS_PARQUET_FALLBACK_PARQUET_STATIC STATIC IMPORTED GLOBAL)
add_dependencies(CS_PARQUET_FALLBACK_ARROW_STATIC external_parquet)
add_dependencies(CS_PARQUET_FALLBACK_PARQUET_STATIC external_parquet)
set_target_properties(CS_PARQUET_FALLBACK_ARROW_STATIC PROPERTIES IMPORTED_LOCATION ${PARQUET_ARROW_LIBRARY})
set_target_properties(CS_PARQUET_FALLBACK_PARQUET_STATIC PROPERTIES IMPORTED_LOCATION ${PARQUET_LIBRARY})

set(ARROW_ENABLED ON)
set(ARROW_PROVIDER "bundled")
set(ARROW_INCLUDE_DIRS "${PARQUET_INSTALL_LOCATION}/include")
set(ARROW_LIBRARIES CS_PARQUET_FALLBACK_ARROW_STATIC)
set(ARROW_EXTERNAL_TARGET external_parquet)

set(PARQUET_ENABLED ON)
set(PARQUET_LIBRARIES CS_PARQUET_FALLBACK_PARQUET_STATIC)
set(PARQUET_EXTERNAL_TARGET external_parquet)
