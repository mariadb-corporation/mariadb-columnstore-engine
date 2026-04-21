# Columnstore-specific CPack overrides applied at package time
# This file is referenced via CPACK_PROJECT_CONFIG_FILE and is included by CPack
# after it reads the generated CPackConfig.cmake, letting these settings win.

# Faster payload compression
set(CPACK_RPM_COMPRESSION_TYPE "zstd")

# Control debuginfo generation (symbols) without debugsource (sources)
option(CS_RPM_DEBUGINFO "Build Columnstore -debuginfo RPM (symbols only)" OFF)

if(CS_RPM_DEBUGINFO)
    # Generate debuginfo RPM (symbols)
    set(CPACK_RPM_DEBUGINFO_PACKAGE ON)
    set(CPACK_RPM_PACKAGE_DEBUG 1)
else()
    # No debuginfo RPM
    set(CPACK_RPM_DEBUGINFO_PACKAGE OFF)
    set(CPACK_RPM_PACKAGE_DEBUG 0)
    set(CPACK_STRIP_FILES OFF)
    # Prevent rpmbuild from stripping binaries and running debug post scripts.
    # CPACK_STRIP_FILES only affects CPack's own stripping; rpmbuild still
    # executes brp-strip and find-debuginfo by default unless we override macros.
    if(DEFINED CPACK_RPM_SPEC_MORE_DEFINE)
        set(CPACK_RPM_SPEC_MORE_DEFINE "${CPACK_RPM_SPEC_MORE_DEFINE}\n%define __strip /bin/true\n%define __objdump /bin/true\n%define __os_install_post %nil\n%define __debug_install_post %nil")
    else()
        set(CPACK_RPM_SPEC_MORE_DEFINE "%define __strip /bin/true\n%define __objdump /bin/true\n%define __os_install_post %nil\n%define __debug_install_post %nil")
    endif()
endif()

# Always disable debugsource by not mapping sources
unset(CPACK_RPM_BUILD_SOURCE_DIRS_PREFIX)
