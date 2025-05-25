macro(SET_FLAGS)
    foreach(F ${ARGV})
        my_check_and_set_compiler_flag(${F} DEBUG RELWITHDEBINFO MINSIZEREL)
    endforeach()
endmacro()

macro(SET_FLAGS_DEBUG)
    foreach(F ${ARGV})
        my_check_and_set_compiler_flag(${F} DEBUG)
    endforeach()
endmacro()

macro(SET_FLAGS_RELEASE)
    foreach(F ${ARGV})
        my_check_and_set_compiler_flag(${F} RELWITHDEBINFO)
    endforeach()
endmacro()

# C++ standard {
if(have_CXX__std_c__20)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20")
else()
    my_check_cxx_compiler_flag("-std=c++2a")
    if(have_CXX__std_c__2a)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++2a")
    else()
        message_once(CS_NO_CXX20 "C++ Compiler does not understand -std=c++20")
        return()
    endif()
endif()

unset(CMAKE_CXX_STANDARD)
# } end C++ standard

# Hacks to keep alive with MariaDB server {
string(REPLACE -D_GLIBCXX_DEBUG "" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
string(REPLACE -D_GLIBCXX_ASSERTIONS "" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
# } end hacks

if(WITH_COLUMNSTORE_ASAN)
    set(WERROR_FLAG)
else()
    set(WERROR_FLAG -Werror)
endif()

# Maintainer flags, works when build is done via bootstrap_mcs.sh {
set(COLUMNSTORE_MAINTAINER_FLAGS ${WERROR_FLAG})
# } end Maintainer flags

# Release, Debug and common flags {
set(FLAGS_ALL
    -Wall
    -Wextra
    -ggdb3
    -fno-omit-frame-pointer
    -fno-strict-aliasing
    -fsigned-char
    -msse4.2
    -DHAVE_CONFIG_H
    -DBOOST_BIND_GLOBAL_PLACEHOLDERS
)

set(FLAGS_RELEASE -O3 -DDBUG_OFF)

set(FLAGS_DEBUG -O0 -D_DEBUG)
# } end Release, Debug and common flags

# linker flags {
set(ENGINE_LDFLAGS "-Wl,--no-as-needed -Wl,--add-needed")
# } end linker flags

# compiler specific flags {
set(CLANG_FLAGS
    # suppressed warnings
    -Wno-cast-function-type-strict
    -Wno-deprecated-copy
    -Wno-deprecated-declarations
    -Wno-deprecated-enum-enum-conversion
    -Wno-format-truncation
    -Wno-register
    -Wno-typedef-redefinition
    -Wno-missing-template-arg-list-after-template-kw
)

set(GNU_FLAGS # suppressed warnings
    -Wno-deprecated-copy -Wno-deprecated-declarations -Wno-format-truncation -Wno-register
)
# } end compiler specific flags

# Sanitizers {
set(ASAN_FLAGS -U_FORTIFY_SOURCE -fsanitize=address -fsanitize-address-use-after-scope -fPIC)
# } end Sanitizers

# configured by cmake/configureEngine.cmake {
if(MASK_LONGDOUBLE)
    my_check_and_set_compiler_flag("-DMASK_LONGDOUBLE")
endif()
# } end configured by cmake/configureEngine.cmake

# apply them all
set_flags(${FLAGS_ALL})
set_flags_debug(${FLAGS_DEBUG})
set_flags_release(${FLAGS_RELEASE})

if(COLUMNSTORE_MAINTAINER)
    set_flags(${COLUMNSTORE_MAINTAINER_FLAGS})
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set_flags(${CLANG_FLAGS})
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set_flags(${GNU_FLAGS})
endif()

if(WITH_COLUMNSTORE_ASAN)
    set_flags(${ASAN_FLAGS})
endif()
