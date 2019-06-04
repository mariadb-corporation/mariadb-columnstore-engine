
include(CheckCXXCompilerFlag)

set(SANATIZE_OPS none CACHE STRING "Use dynamic sanitizer")
set_property(CACHE SANATIZE_OPS PROPERTY STRINGS none address pointer-compare thread leak undefined)

if("none" STREQUAL ${SANATIZE_OPS})
  return()
endif()

set(SANATIZE_FLAGS -fsanatize=${SANATIZE_OPS})
set(CMAKE_REQUIRED_FLAGS ${SANATIZE_FLAGS})
check_cxx_compiler_flag(${SANATIZE_FLAGS} sanitizer_support)
unset(CMAKE_REQUIRED_FLAGS)

if(NOT sanitizer_support)
  warning("Compiler does not support sanitizer ${SANATIZE_OPS}")
  return()
endif()

add_compile_options(${SANATIZE_FLAGS})
