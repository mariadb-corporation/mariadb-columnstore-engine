# Build SELinux policy and package it for RPM on RHEL-like systems >= 10 only
# Builds from: storage/columnstore/columnstore/build/security/columnstore.te
# Produces: columnstore.pp packaged under ${ENGINE_SUPPORTDIR}/policy/selinux
# Adds BuildRequires: selinux-policy-devel (RPM, RHEL-like >= 10)

# Detect if we are building an RPM package
if(NOT RPM)
    return()
endif()

columnstore_detect_os(_os_id _os_version_major)
columnstore_is_rhel_like("${_os_id}" _is_rhel_like)

# We only build on RHEL-like >= 10
if(NOT _is_rhel_like
   OR (NOT _os_version_major)
   OR (_os_version_major LESS 10)
)
    message(
        STATUS
            "SELinux policy build skipped: OS '${_os_id}' version '${_os_version_major}' not matching RHEL-like >= 10 or undetected."
    )
    return()
endif()

# Add RPM BuildRequires for the engine component only on matching systems Use the common appender macro to handle comma
# separation
columnstore_append_for_cpack(CPACK_RPM_columnstore-engine_PACKAGE_BUILDREQUIRES "selinux-policy-devel")

# Paths
set(SELINUX_SRC_DIR "${CMAKE_CURRENT_LIST_DIR}/../build/security")
set(SELINUX_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/selinux")
set(SELINUX_TE "${SELINUX_SRC_DIR}/columnstore.te")
set(SELINUX_PP "${SELINUX_BUILD_DIR}/columnstore.pp")

file(MAKE_DIRECTORY "${SELINUX_BUILD_DIR}")

# Ensure selinux-policy-devel is available
if(NOT EXISTS "/usr/share/selinux/devel/Makefile")
    message(
        FATAL_ERROR
            "SELinux policy build requires '/usr/share/selinux/devel/Makefile'. Please install 'selinux-policy-devel' (RHEL/Rocky >= 10) and re-run CMake."
    )
endif()

# Custom command to build the .pp from .te using the upstream devel Makefile
add_custom_command(
    OUTPUT "${SELINUX_PP}"
    COMMAND ${CMAKE_COMMAND} -E copy "${SELINUX_TE}" "${SELINUX_BUILD_DIR}/columnstore.te"
    COMMAND make -f /usr/share/selinux/devel/Makefile columnstore.pp
    WORKING_DIRECTORY "${SELINUX_BUILD_DIR}"
    DEPENDS "${SELINUX_TE}"
    COMMENT "Building SELinux policy columnstore.pp from columnstore.te"
    VERBATIM
)

add_custom_target(selinux_policy ALL DEPENDS "${SELINUX_PP}")

# Install the compiled policy into the package (no runtime dep). Post-install will load it conditionally.
install(
    FILES "${SELINUX_PP}"
    DESTINATION "${ENGINE_SUPPORTDIR}/policy/selinux"
    COMPONENT columnstore-engine
)

# Register RPM post-install and post-uninstall scripts for the component
set(_selinux_post "${CMAKE_CURRENT_LIST_DIR}/../build/selinux_policy_rpm_post.sh")
set(_selinux_postun "${CMAKE_CURRENT_LIST_DIR}/../build/selinux_policy_rpm_postun.sh")

# POST_INSTALL: preserve existing script if set by wrapping it
if(EXISTS "${_selinux_post}")
    if(DEFINED CPACK_RPM_columnstore-engine_POST_INSTALL_SCRIPT_FILE
       AND CPACK_RPM_columnstore-engine_POST_INSTALL_SCRIPT_FILE
    )
        set(_orig_post "${CPACK_RPM_columnstore-engine_POST_INSTALL_SCRIPT_FILE}")
        set(_wrap_post "${SELINUX_BUILD_DIR}/post_install_wrapper.sh")
        file(WRITE "${_wrap_post}" "#!/bin/sh\n\n'${_orig_post}' \"$@\" || true\n'${_selinux_post}' \"$@\" || true\n")
        execute_process(COMMAND ${CMAKE_COMMAND} -E chmod +x "${_wrap_post}")
        set(CPACK_RPM_columnstore-engine_POST_INSTALL_SCRIPT_FILE "${_wrap_post}")
    else()
        set(CPACK_RPM_columnstore-engine_POST_INSTALL_SCRIPT_FILE "${_selinux_post}")
    endif()
endif()

# POST_UNINSTALL: preserve existing script if set by wrapping it
if(EXISTS "${_selinux_postun}")
    if(DEFINED CPACK_RPM_columnstore-engine_POST_UNINSTALL_SCRIPT_FILE
       AND CPACK_RPM_columnstore-engine_POST_UNINSTALL_SCRIPT_FILE
    )
        set(_orig_postun "${CPACK_RPM_columnstore-engine_POST_UNINSTALL_SCRIPT_FILE}")
        set(_wrap_postun "${SELINUX_BUILD_DIR}/post_uninstall_wrapper.sh")
        file(WRITE "${_wrap_postun}"
             "#!/bin/sh\n\n'${_orig_postun}' \"$@\" || true\n'${_selinux_postun}' \"$@\" || true\n"
        )
        execute_process(COMMAND ${CMAKE_COMMAND} -E chmod +x "${_wrap_postun}")
        set(CPACK_RPM_columnstore-engine_POST_UNINSTALL_SCRIPT_FILE "${_wrap_postun}")
    else()
        set(CPACK_RPM_columnstore-engine_POST_UNINSTALL_SCRIPT_FILE "${_selinux_postun}")
    endif()
endif()
