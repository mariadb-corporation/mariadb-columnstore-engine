# Read value for a variable from VERSION.
macro(MYSQL_GET_CONFIG_VALUE keyword var)
    if(NOT ${var})
        file(STRINGS ${SERVER_SOURCE_DIR}/VERSION str REGEX "^[ ]*${keyword}=")
        if(str)
            string(REPLACE "${keyword}=" "" str ${str})
            string(REGEX REPLACE "[ ].*" "" str "${str}")
            set(${var} ${str})
        endif()
    endif()
endmacro()

function(get_linux_lsb_release_information)
    # Try lsb_release first
    find_program(LSB_RELEASE_EXEC lsb_release)

    if(LSB_RELEASE_EXEC)
        execute_process(
            COMMAND "${LSB_RELEASE_EXEC}" --short --id
            OUTPUT_VARIABLE LSB_RELEASE_ID_SHORT
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        string(TOLOWER "${LSB_RELEASE_ID_SHORT}" LSB_RELEASE_ID_SHORT)

        execute_process(
            COMMAND "${LSB_RELEASE_EXEC}" --short --release
            OUTPUT_VARIABLE LSB_RELEASE_VERSION_SHORT
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )

        execute_process(
            COMMAND "${LSB_RELEASE_EXEC}" --short --codename
            OUTPUT_VARIABLE LSB_RELEASE_CODENAME_SHORT
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
    else()
        # Fallback: parse /etc/os-release
        if(EXISTS "/etc/os-release")
            file(READ "/etc/os-release" OS_RELEASE_CONTENT)

            string(REGEX MATCH "ID=([^\n]*)" _match "${OS_RELEASE_CONTENT}")
            set(LSB_RELEASE_ID_SHORT "${CMAKE_MATCH_1}")
            # Remove quotes if present
            string(REGEX REPLACE "^\"(.*)\"$" "\\1" LSB_RELEASE_ID_SHORT "${LSB_RELEASE_ID_SHORT}")
            string(TOLOWER "${LSB_RELEASE_ID_SHORT}" LSB_RELEASE_ID_SHORT)

            string(REGEX MATCH "VERSION_ID=([^\n]*)" _match "${OS_RELEASE_CONTENT}")
            set(LSB_RELEASE_VERSION_SHORT "${CMAKE_MATCH_1}")
            # Remove quotes if present
            string(REGEX REPLACE "^\"(.*)\"$" "\\1" LSB_RELEASE_VERSION_SHORT "${LSB_RELEASE_VERSION_SHORT}")

            string(REGEX MATCH "VERSION_CODENAME=([^\n]*)" _match "${OS_RELEASE_CONTENT}")
            if(CMAKE_MATCH_1)
                set(LSB_RELEASE_CODENAME_SHORT "${CMAKE_MATCH_1}")
                # Remove quotes if present
                string(REGEX REPLACE "^\"(.*)\"$" "\\1" LSB_RELEASE_CODENAME_SHORT "${LSB_RELEASE_CODENAME_SHORT}")
            else()
                set(LSB_RELEASE_CODENAME_SHORT "")
            endif()
        else()
            message(FATAL_ERROR "Could not detect lsb_release or /etc/os-release, cannot gather required information")
        endif()
    endif()

    message(STATUS "LSB_RELEASE_ID_SHORT ${LSB_RELEASE_ID_SHORT}")
    message(STATUS "LSB_RELEASE_VERSION_SHORT ${LSB_RELEASE_VERSION_SHORT}")
    message(STATUS "LSB_RELEASE_CODENAME_SHORT ${LSB_RELEASE_CODENAME_SHORT}")

    set(LSB_RELEASE_ID_SHORT
        "${LSB_RELEASE_ID_SHORT}"
        PARENT_SCOPE
    )
    set(LSB_RELEASE_VERSION_SHORT
        "${LSB_RELEASE_VERSION_SHORT}"
        PARENT_SCOPE
    )
    set(LSB_RELEASE_CODENAME_SHORT
        "${LSB_RELEASE_CODENAME_SHORT}"
        PARENT_SCOPE
    )
endfunction()

# Read mysql version for configure script
macro(GET_MYSQL_VERSION)
    mysql_get_config_value("MYSQL_VERSION_MAJOR" MAJOR_VERSION)
    mysql_get_config_value("MYSQL_VERSION_MINOR" MINOR_VERSION)
    mysql_get_config_value("MYSQL_VERSION_PATCH" PATCH_VERSION)
    mysql_get_config_value("MYSQL_VERSION_EXTRA" EXTRA_VERSION)
    mysql_get_config_value("SERVER_MATURITY" SERVER_MATURITY)

    if(NOT "${MAJOR_VERSION}" MATCHES "[0-9]+"
       OR NOT "${MINOR_VERSION}" MATCHES "[0-9]+"
       OR NOT "${PATCH_VERSION}" MATCHES "[0-9]+"
    )
        message(FATAL_ERROR "VERSION file cannot be parsed.")
    endif()
    if((NOT TINY_VERSION) AND (EXTRA_VERSION MATCHES "[\\-][0-9]+"))
        string(REPLACE "-" "" TINY_VERSION "${EXTRA_VERSION}")
    else()
        set(TINY_VERSION "0")
    endif()
    set(VERSION "${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}${EXTRA_VERSION}")
    set(SERVER_VERSION ${VERSION})
    message(STATUS "MariaDB ${VERSION}")
    set(MYSQL_BASE_VERSION
        "${MAJOR_VERSION}.${MINOR_VERSION}"
        CACHE INTERNAL "MySQL Base version"
    )
    set(MYSQL_NO_DASH_VERSION "${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}")
    math(EXPR MYSQL_VERSION_ID "10000*${MAJOR_VERSION} + 100*${MINOR_VERSION} + ${PATCH_VERSION}")
    mark_as_advanced(VERSION MYSQL_VERSION_ID MYSQL_BASE_VERSION)
    set(CPACK_PACKAGE_VERSION_MAJOR ${MAJOR_VERSION})
    set(CPACK_PACKAGE_VERSION_MINOR ${MINOR_VERSION})
    set(CPACK_PACKAGE_VERSION_PATCH ${PATCH_VERSION}${EXTRA_VERSION})
endmacro()
