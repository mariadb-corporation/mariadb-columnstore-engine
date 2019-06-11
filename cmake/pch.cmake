
option(USE_PCH "reduce compile time with precompiled headers" FALSE)
set(COTIRE_ENABLE_PRECOMPILED_HEADER ${USE_PCH} CACHE BOOL "" FORCE)
set(COTIRE_VERBOSE ${CMAKE_VERBOSE_MAKEFILE} CACHE BOOL "" FORCE)

mark_as_advanced(COTIRE_ENABLE_PRECOMPILED_HEADER COTIRE_VERBOSE)

if(NOT USE_PCH)
  function(target_pch)
    return()
  endfunction()
  return()
endif()

set(COTIRE_DOWNLOAD_DIR ${CMAKE_CURRENT_BINARY_DIR}/.cotire)
set(COTIRE_DIR ${CMAKE_CURRENT_BINARY_DIR}/cotire)
list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${COTIRE_DOWNLOAD_DIR};${COTIRE_DIR})

configure_file(${CMAKE_CURRENT_LIST_DIR}/pch.CMakeLists.txt ${COTIRE_DOWNLOAD_DIR}/CMakeLists.txt @ONLY)

execute_process(COMMAND ${CMAKE_COMMAND} . WORKING_DIRECTORY ${COTIRE_DOWNLOAD_DIR})

execute_process(COMMAND ${CMAKE_COMMAND} --build . WORKING_DIRECTORY ${COTIRE_DOWNLOAD_DIR})

LIST(APPEND CMAKE_MODULE_PATH ${COTIRE_DIR}/CMake)
include(cotire)

#[[
  target_pch( <target name> [ <pch header> ] )

  if the PCH header parameter is missing they will be automatically deduced
]]

function(target_pch _target)
  get_target_property(_sources ${_target} SOURCES)
  get_target_property(_srcdir ${_target} SOURCE_DIR)
  get_target_property(_type ${_target} TYPE)
  set(_pch_h_exists FALSE)
  foreach(_src IN LISTS _sources)
    if("pch.h" STREQUAL "${_src}")
     set(_pch_h_exists FALSE)
     break()
    endif()
  endforeach()

  if(_pch_h_exists)
    set_target_properties(${_target} PROPERTIES COTIRE_CXX_PREFIX_HEADER_INIT "pch.h")
  elseif("STATIC_LIBRARY" STREQUAL "${_type}" OR "OBJECT_LIBRARY" STREQUAL "${_type}")
    return()
  else()
    set_target_properties(${_target} PROPERTIES COTIRE_CXX_PREFIX_HEADER_INIT "${columnstore_BINARY_DIR}/columnstore.h")
  endif()

  cotire(${_target})
endfunction()
