find_package(BISON)
if(NOT BISON_FOUND)
    message_once(CS_NO_BISON "bison not found!")
    return()
endif()

find_program(LEX_EXECUTABLE flex DOC "path to the flex executable")
if(NOT LEX_EXECUTABLE)
    find_program(LEX_EXECUTABLE lex DOC "path to the lex executable")
    if(NOT LEX_EXECUTABLE)
        message_once(CS_NO_LEX "flex/lex not found!")
        return()
    endif()
endif()

find_package(LibXml2)
if(NOT LIBXML2_FOUND)
    message_once(CS_NO_LIBXML "Could not find a usable libxml2 development environment!")
    return()
endif()

find_package(Snappy)
if(NOT SNAPPY_FOUND)
    message_once(
        CS_NO_SNAPPY
        "Snappy not found please install snappy-devel for CentOS/RedHat or libsnappy-dev for Ubuntu/Debian"
    )
    return()
endif()

find_package(CURL)
if(NOT CURL_FOUND)
    message_once(CS_NO_CURL "libcurl development headers not found")
    return()
endif()

find_program(AWK_EXECUTABLE awk DOC "path to the awk executable")
if(NOT AWK_EXECUTABLE)
    message_once(CS_NO_AWK "awk not found!")
    return()
endif()

set(HAVE_LZ4
    0
    CACHE INTERNAL ""
)
if(WITH_COLUMNSTORE_LZ4 STREQUAL "ON" OR WITH_COLUMNSTORE_LZ4 STREQUAL "AUTO")
    find_package(LZ4)
    if(NOT LZ4_FOUND)
        if(WITH_COLUMNSTORE_LZ4 STREQUAL "AUTO")
            message_once(CS_LZ4 "LZ4 not found, building without LZ4")
        else()
            message(FATAL_ERROR "LZ4 not found.")
        endif()
    else()
        message_once(CS_LZ4 "Building with LZ4")
        set(HAVE_LZ4
            1
            CACHE INTERNAL ""
        )
    endif()
else()
    message_once(CS_LZ4 "Building without LZ4")
endif()
