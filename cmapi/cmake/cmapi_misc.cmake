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

            string(REGEX MATCH "(^|\n)ID=([^\n]*)" _match "${OS_RELEASE_CONTENT}")
            set(LSB_RELEASE_ID_SHORT "${CMAKE_MATCH_2}")
            # Remove quotes if present
            string(REGEX REPLACE "^\"(.*)\"$" "\\1" LSB_RELEASE_ID_SHORT "${LSB_RELEASE_ID_SHORT}")
            string(TOLOWER "${LSB_RELEASE_ID_SHORT}" LSB_RELEASE_ID_SHORT)

            string(REGEX MATCH "(^|\n)VERSION_ID=([^\n]*)" _match "${OS_RELEASE_CONTENT}")
            set(LSB_RELEASE_VERSION_SHORT "${CMAKE_MATCH_2}")
            # Remove quotes if present
            string(REGEX REPLACE "^\"(.*)\"$" "\\1" LSB_RELEASE_VERSION_SHORT "${LSB_RELEASE_VERSION_SHORT}")

            string(REGEX MATCH "(^|\n)VERSION_CODENAME=([^\n]*)" _match "${OS_RELEASE_CONTENT}")
            if(_match)
                set(LSB_RELEASE_CODENAME_SHORT "${CMAKE_MATCH_2}")
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
