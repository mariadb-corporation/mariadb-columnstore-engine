
include(CheckCXXCompilerFlag)


add_link_options(-Wl,--demangle)

#remove dead code & data
option(REMOVE_UNUSED_SYMS "Remove unused symbols from binaries" TRUE)
cmake_dependent_option(PRINT_REMOVED_SYMS "Print the symbols that are removed from binaries" TRUE "REMOVE_UNUSED_SYMS" FALSE)
add_compile_options($<$<BOOL:${REMOVE_UNUSED_SYMS}>:-ffunction-sections>)
add_compile_options($<$<BOOL:${REMOVE_UNUSED_SYMS}>:-fdata-sections>)
add_link_options($<$<BOOL:${REMOVE_UNUSED_SYMS}>:-Wl,--gc-sections>)
add_link_options($<$<BOOL:${PRINT_REMOVED_SYMS}>:-Wl,--print-gc-sections>)


add_compile_options(-fuse-ld=bfd)
#[[
#use gold linker (faster than LD)
set(USE_GOLD_FLAGS -fuse-ld=gold)
set(CMAKE_REQUIRED_FLAGS ${USE_GOLD_FLAGS})
check_cxx_compiler_flag(${USE_GOLD_FLAGS} gold_linker_support)
unset(CMAKE_REQUIRED_FLAGS)
cmake_dependent_option(USE_GOLD_LD "Use GOLD linker" TRUE ${gold_linker_support} FALSE)
add_compile_options($<$<BOOL:${USE_GOLD_LD}>:-fuse-ld=gold>)
mark_as_advanced(USE_GOLD_LD)
]]

# warnings
set(WARNING_LEVEL all CACHE STRING "Compiler warning level")
set_property(CACHE WARNING_LEVEL PROPERTY STRINGS 0 all extra pedantic)
if("0" STREQUAL "${WARNING_LEVEL}")
  add_compile_options(-w)
elseif("all" STREQUAL "${WARNING_LEVEL}")
  add_compile_options(-Wall)
elseif("extra" STREQUAL "${WARNING_LEVEL}")
  add_compile_options(-Wall -Wextra)
else()
  add_compile_options(-pedantic)
endif()

option(_WERROR "Treat warnings as errors" FALSE)
add_compile_options($<$<BOOL:${_WERROR}>:-Werror>)


# optimization
set(_default_optimization 3)
if(_DEBUG)
  set(_default_optimization 1)
endif()
set(OPTIMIZATION_LEVEL ${_default_optimization} CACHE STRING "Compiler optimization level")
set_property(CACHE OPTIMIZATION_LEVEL PROPERTY STRINGS 0 1 2 3)
add_compile_options(-O${OPTIMIZATION_LEVEL})

option(OMIT_FRAME_POINTERS "Omit frame pointers" FALSE)
add_compile_options($<IF:$<BOOL:${OMIT_FRAME_POINTERS}>,-fomit-frame-pointer,-fno-omit-frame-pointer>)
mark_as_advanced(OMIT_FRAME_POINTERS)



# CRT settings
cmake_dependent_option(_GLIBCXX_ASSERTIONS "Enable runtime assertions" TRUE "_DEBUG" FALSE)
add_compile_definitions($<$<BOOL:${_GLIBCXX_ASSERTIONS}>:_GLIBCXX_ASSERTIONS>)

cmake_dependent_option(_GLIBCXX_DEBUG "Enable STL debugging (Implies _GLIBCXX_ASSERTIONS)" TRUE "_DEBUG" FALSE)
add_compile_definitions($<$<BOOL:${_GLIBCXX_DEBUG}>:_GLIBCXX_DEBUG>)

cmake_dependent_option(_GLIBCXX_DEBUG_PEDANTIC "Enable pedantic STL debugging" TRUE "_GLIBCXX_DEBUG" FALSE)
add_compile_definitions($<$<BOOL:${_GLIBCXX_DEBUG_PEDANTIC}>:_GLIBCXX_DEBUG_PEDANTIC>)

option(_GLIBCXX_CONCEPT_CHECKS "Check for standard violations in template instantiations during compile" FALSE)
add_compile_definitions($<$<BOOL:${_GLIBCXX_CONCEPT_CHECKS}>:_GLIBCXX_CONCEPT_CHECKS>)

option(_GLIBCXX_PROFILE "Enable libstdc++ profiling" FALSE)
add_compile_definitions($<$<BOOL:${_GLIBCXX_PROFILE}>:_GLIBCXX_PROFILE>)

message(AUTHOR_WARNING "TODO: Enable runtime checks")
#[[
INCLUDE(check_compiler_flag)

MY_CHECK_AND_SET_COMPILER_FLAG("-g -O3 -fno-omit-frame-pointer -fno-strict-aliasing -Wall -fno-tree-vectorize -D_GLIBCXX_ASSERTIONS -DDBUG_OFF -DHAVE_CONFIG_H" RELEASE RELWITHDEBINFO MINSIZEREL)
MY_CHECK_AND_SET_COMPILER_FLAG("-ggdb3 -fno-omit-frame-pointer -fno-tree-vectorize -D_GLIBCXX_ASSERTIONS -DSAFE_MUTEX -DSAFEMALLOC -DENABLED_DEBUG_SYNC -O0 -Wall -D_DEBUG -DHAVE_CONFIG_H" DEBUG)

# enable security hardening features, like most distributions do
# in our benchmarks that costs about ~1% of performance, depending on the load
IF(CMAKE_C_COMPILER_VERSION VERSION_LESS "4.6")
  SET(security_default OFF)
ELSE()
  SET(security_default ON)
ENDIF()
OPTION(SECURITY_HARDENED "Use security-enhancing compiler features (stack protector, relro, etc)" ${security_default})
OPTION(SECURITY_HARDENED_NEW "Use new security-enhancing compilier features" OFF)
IF(SECURITY_HARDENED)
  # security-enhancing flags
  MY_CHECK_AND_SET_COMPILER_FLAG("-pie -fPIC")
  MY_CHECK_AND_SET_COMPILER_FLAG("-Wl,-z,relro,-z,now")
  MY_CHECK_AND_SET_COMPILER_FLAG("-fstack-protector --param=ssp-buffer-size=4")
  MY_CHECK_AND_SET_COMPILER_FLAG("-D_FORTIFY_SOURCE=2" RELEASE RELWITHDEBINFO)
  MY_CHECK_AND_SET_COMPILER_FLAG("-fexceptions")
  IF(SECURITY_HARDENED_NEW)
    MY_CHECK_AND_SET_COMPILER_FLAG("-mcet -fcf-protection")
    MY_CHECK_AND_SET_COMPILER_FLAG("-fstack-protector-strong")
    MY_CHECK_AND_SET_COMPILER_FLAG("-fstack-clash-protection")
  ENDIF()
ENDIF()
]]