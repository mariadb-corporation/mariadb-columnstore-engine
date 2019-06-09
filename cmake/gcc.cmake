
function(set_compile_options _switch)
  set(CMAKE_REQUIRED_FLAGS ${_switch})
  string(REPLACE "-" "_" switch_name ${_switch})
  string(REPLACE "," "_" switch_name ${_switch})
  set(switch_name ${switch_name}_supported)
  check_cxx_compiler_flag(${_switch} ${switch_name})
  unset(CMAKE_REQUIRED_FLAGS)
  add_compile_options($<$<BOOL:${${switch_name}}>:${_switch}>)
endfunction()

# always on
add_compile_options(-pipe)
add_compile_options(-march=core2)
add_compile_options(-mtune=generic)
add_compile_options(-mfpmath=sse)
add_compile_options(-fuse-cxa-atexit)
add_compile_options($<$<CONFIG:Debug>:-ggdb3>)
add_compile_options(-Wl,--demangle) # demangle names in error messages
add_compile_options(-Wl,--cref) # generate cross reference is map file
add_compile_options(-Wl,-z,combreloc) # Combine multiple dynamic relocation sections and sort to improve dynamic symbol lookup caching
add_compile_options(-Wl,-z,now) # resolve all symbols when loaded or started rather than on-demand
add_compile_options(-Wl,-z,relro) # mark segments are read-only after relocation
add_compile_options(-Wl,-map) # generate map file

option(ENABLE_POSITION_INDEPENDENT_CODE "Enable position independent code" TRUE)
mark_as_advanced(ENABLE_POSITION_INDEPENDENT_CODE)
set(CMAKE_POSITION_INDEPENDENT_CODE ${ENABLE_POSITION_INDEPENDENT_CODE})
add_compile_options($<$<BOOL:${ENABLE_POSITION_INDEPENDENT_CODE}>:-pie>)


option(ENABLE_STACK_PROTECTION "Enable stack protection" TRUE)
mark_as_advanced(ENABLE_STACK_PROTECTION)
add_compile_options($<IF:$<BOOL:${ENABLE_STACK_PROTECTION}>,-fstack-protector-strong$<SEMICOLON>--param=ssp-buffer-size=4,-fno-stack-protector>)

add_compile_definitions($<$<BOOL:${ENABLE_STACK_PROTECTION}>:_FORTIFY_SOURCE=2>)


set(ENABLE_CONTROL_FLOW_PROTECTION full CACHE STRING "Enable control-flow protection")
set_property(CACHE ENABLE_CONTROL_FLOW_PROTECTION PROPERTY STRINGS full branch return none)
mark_as_advanced(ENABLE_CONTROL_FLOW_PROTECTION)
set_compile_options(-fcf-protection=${ENABLE_CONTROL_FLOW_PROTECTION})


if("${CMAKE_COMPILER_VERSION}" VERSION_GREATER_EQUAL "8.0")
  option(ENABLE_STACK_CLASH_PROTECTION "Protect from stack-clash attacks" TRUE)
  mark_as_advanced(ENABLE_STACK_CLASH_PROTECTION)
  add_compile_options($<$<BOOL:${ENABLE_STACK_CLASH_PROTECTION}>:-fstack-clash-protection>)
endif()


# warnings
set(WARNING_LEVEL all CACHE STRING "Compiler warning level")
set_property(CACHE WARNING_LEVEL PROPERTY STRINGS 0 1 2 3)
if("0" STREQUAL "${WARNING_LEVEL}")
  set_compile_options(-w)
elseif("1" STREQUAL "${WARNING_LEVEL}")
  set_compile_options(-Wall)
elseif("2" STREQUAL "${WARNING_LEVEL}")
  set_compile_options(-Wall)
  set_compile_options(-Wextra)
else()
  set_compile_options(-Wall)
  set_compile_options(-Wextra)
  set_compile_options(-pedantic)
  set_compile_options(-Wnoexcept)
  set_compile_options(-Wnon-virtual-dtor)
  set_compile_options(-Wreorder)
  set_compile_options(-Weffc++)
  set_compile_options(-Wstrict-null-sentinel)
  set_compile_options(-Wold-style-cast)
  set_compile_options(-Woverloaded-virtual)
  set_compile_options(-Wmultiple-inheritance )
  set_compile_options(-Wvirtual-inheritance)
  set_compile_options(-Wnamespaces)
  set_compile_options(-Wno-terminate )
  set_compile_options(-Wchkp)
  set_compile_options(-Wmissing-include-dirsz)
  set_compile_options(-Wswitch-default)
  set_compile_options(-Wswitch-enum)
  set_compile_options(-Wsync-nand)
  set_compile_options(-Wunused)
  set_compile_options(-Wuninitialized)
  set_compile_options(-Wunknown-pragmas)
  set_compile_options(-Wduplicated-cond)
  set_compile_options(-Wcast-align)
  set_compile_options(-Wstack-protector)
endif()


option(ENABLE_PEDANTIC_WARNINGS "Enable pedantic warnings" FALSE)
add_compile_options($<$<BOOL:${ENABLE_PEDANTIC_WARNINGS}>:-pedantic>)


option(ENABLE_WERROR "Treat warnings as errors" FALSE)
add_compile_options($<$<BOOL:${ENABLE_WERROR}>:-Werror>)


option(ENABLE_EXCEPTIONS "Enable c++ exceptions" TRUE)
add_compile_options($<$<BOOL:${ENABLE_EXCEPTIONS}>:-fexceptions>)
mark_as_advanced(ENABLE_EXCEPTIONS)


#remove dead code & data
option(REMOVE_UNUSED_SYMS "Remove unused symbols from binaries" TRUE)
mark_as_advanced(REMOVE_UNUSED_SYMS)
cmake_dependent_option(PRINT_REMOVED_SYMS "Print the symbols that are removed from binaries" TRUE "REMOVE_UNUSED_SYMS" FALSE)
add_compile_options($<$<BOOL:${REMOVE_UNUSED_SYMS}>:-ffunction-sections>)
add_compile_options($<$<BOOL:${REMOVE_UNUSED_SYMS}>:-fdata-sections>)
add_compile_options($<$<BOOL:${REMOVE_UNUSED_SYMS}>:-Wl,--gc-sections>)
add_compile_options($<$<BOOL:${PRINT_REMOVED_SYMS}>:-Wl,--print-gc-sections>)


set(USE_LINKER ld CACHE STRING "Specify alternate linker")
mark_as_advanced(USE_LINKER)
set_property(CACHE USE_LINKER PROPERTY STRINGS ld gold bfd)
if (NOT "ld" STREQUAL "${USE_LINKER}")
  add_compile_options(-fuse-ld=${USE_LINKER})
endif()


option(OMIT_FRAME_POINTERS "Omit frame pointers" FALSE)
mark_as_advanced(OMIT_FRAME_POINTERS)
add_compile_options($<IF:$<BOOL:${OMIT_FRAME_POINTERS}>,-fomit-frame-pointer,-fno-omit-frame-pointer>)


option(ENABLE_LTO "Enable link-time optimizations" TRUE)
mark_as_advanced(ENABLE_LTO)
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ${ENABLE_LTO})
if(ENABLE_LTO)
  add_compile_options(-flto)
  add_compile_options(-Wl,-flto)
  add_compile_options(-Wl,-fuse-linker-plugin)
  
  set(CMAKE_C_ARCHIVE_CREATE  <CMAKE_AR> qcs <TARGET> <LINK_FLAGS> <OBJECTS>)
  set(CMAKE_C_ARCHIVE_FINISH true)
  set(CMAKE_AR "gcc-ar")  
endif()


option(ENABLE_STRICT_ALIASING "Enable strict aliasing of pointers" FALSE)
mark_as_advanced(ENABLE_STRICT_ALIASING)
add_compile_options($<IF:$<BOOL:${ENABLE_STRICT_ALIASING}>,-fstrict-aliasing,-fno-strict-aliasing>)


option(ENABLE_TREE_VECTORIZATION "Perform vectorization on trees" FALSE)
mark_as_advanced(ENABLE_TREE_VECTORIZATION)
add_compile_options($<IF:$<BOOL:${ENABLE_TREE_VECTORIZATION}>,-ftree-vectorize,-fno-tree-vectorize>)




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
