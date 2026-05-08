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

message(FATAL_ERROR
    "Parquet requested but not found. "
    "Use WITH_ARROW=bundled (or auto without a system Arrow hit) so arrow.cmake "
    "provides bundled Parquet targets, or install system Parquet.")
