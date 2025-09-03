macro(columnstore_append_for_cpack var_name)
    # Get current value from parent scope or use empty string
    if(DEFINED ${var_name})
        set(current_val "${${var_name}}")
    else()
        set(current_val "")
    endif()

    # Process each argument to append
    foreach(arg IN LISTS ARGN)
        if(current_val)
            # If not empty, add comma before new item
            set(current_val "${current_val}, ${arg}")
        else()
            # If empty, just add the item
            set(current_val "${arg}")
        endif()
    endforeach()

    # Set back in parent scope
    set(${var_name}
        "${current_val}"
        PARENT_SCOPE
    )
endmacro()

macro(columnstore_add_rpm_deps)
    columnstore_append_for_cpack(CPACK_RPM_columnstore-engine_PACKAGE_REQUIRES ${ARGN})
endmacro()

if(NOT COLUMNSTORE_MAINTAINER)
    return()
endif()

# Columnstore-specific RPM packaging overrides 1) Use fast compression to speed up packaging
set(CPACK_RPM_COMPRESSION_TYPE
    "zstd"
    CACHE STRING "RPM payload compression" FORCE
)
# 2) Disable debuginfo/debugsource to avoid slow packaging and duplicate file warnings
set(CPACK_RPM_DEBUGINFO_PACKAGE
    OFF
    CACHE BOOL "Disable debuginfo package" FORCE
)
set(CPACK_RPM_PACKAGE_DEBUG
    0
    CACHE STRING "Disable RPM debug package" FORCE
)
unset(CPACK_RPM_BUILD_SOURCE_DIRS_PREFIX CACHE)

# Ensure our overrides are applied by CPack at packaging time CPACK_PROJECT_CONFIG_FILE is included by cpack after
# CPackConfig.cmake is loaded
set(CPACK_PROJECT_CONFIG_FILE
    "${CMAKE_CURRENT_LIST_DIR}/cpack_overrides.cmake"
    CACHE FILEPATH "Columnstore CPack overrides" FORCE
)

columnstore_add_rpm_deps("snappy" "jemalloc" "procps-ng" "gawk")
