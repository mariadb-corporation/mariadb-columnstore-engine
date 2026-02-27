if(NOT RPM)
    return()
endif()

# Define %ignore as a comment so RPM doesn't claim ownership of shared directories
set(CPACK_RPM_SPEC_MORE_DEFINE "${CPACK_RPM_SPEC_MORE_DEFINE}
%define ignore \#
")

set(ignored
    "%ignore /usr"
    "%ignore /usr/local"
    "%ignore /bin"
    "%ignore /lib"
    "%ignore /usr/sbin"
    "%ignore /usr/lib64/mysql"
    "%ignore /usr/lib64/mysql/plugin"
    # REMOVED: "%ignore /etc/my.cnf.d" <--- This was causing the conflict
    "%ignore /var/lib"
    "%ignore /var"
)

# Apply it to the specific component
set(CPACK_RPM_columnstore-engine_USER_FILELIST "${ignored}")

macro(columnstore_append_for_cpack var_name)
    # Process each argument passed to the macro
    foreach(arg IN LISTS ARGN)
        if(${var_name})
            # Use a semicolon (;) for CPack lists, NOT a comma
            set(${var_name} "${${var_name}};${arg}" PARENT_SCOPE)
        else()
            set(${var_name} "${arg}" PARENT_SCOPE)
        endif()
    endforeach()
endmacro()

macro(columnstore_add_rpm_deps)
    columnstore_append_for_cpack(CPACK_RPM_columnstore-engine_PACKAGE_REQUIRES ${ARGN})
endmacro()

columnstore_add_rpm_deps("snappy" "jemalloc" "procps-ng" "gawk")

if(COLUMNSTORE_MAINTAINER)
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
endif() # COLUMNSTORE_MAINTAINER
