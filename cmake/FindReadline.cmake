
if(NOT Readline_ROOT)
  set(Readline_ROOT "$ENV{Readline_ROOT}")
endif()

if(NOT Readline_ROOT)
  find_path(Readline_ROOT NAMES include/readline/readline.h)
endif()

find_path(Readline_INCLUDE_DIRS NAMES readline/readline.h HINTS ${Readline_ROOT}/include)

find_library(Readline_LIBRARIES NAMES readline HINTS ${Readline_ROOT}/lib64 ${Readline_ROOT}/lib )

mark_as_advanced(Readline_ROOT Readline_INCLUDE_DIRS Readline_LIBRARIES)


file(STRINGS ${Readline_INCLUDE_DIRS}/readline/readline.h _RL_VERS REGEX "^[ \t]*#define[ \t]+RL_VERSION_(MAJOR|MINOR)")
if(_RL_VERS)
  string(REGEX REPLACE ".*RL_VERSION_MAJOR[ \t]*([0-9]+).*" "\\1" _RL_VER_MAJOR "${_RL_VERS}") 
  string(REGEX REPLACE ".*RL_VERSION_MINOR[ \t]*([0-9]+).*" "\\1" _RL_VER_MINOR "${_RL_VERS}") 
  set(Readline_VERSION "${_RL_VER_MAJOR}.${_RL_VER_MINOR}")
endif()


include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(Readline 
  REQUIRED_VARS Readline_INCLUDE_DIRS Readline_LIBRARIES
  VERSION_VAR Readline_VERSION
  )

add_library(Readline::Readline UNKNOWN IMPORTED)
set_target_properties(Readline::Readline PROPERTIES 
  IMPORTED_LOCATION ${Readline_LIBRARIES}
  IMPORTED_LINK_INTERFACE_LANGUAGES "C"
  INTERFACE_INCLUDE_DIRECTORIES ${Readline_INCLUDE_DIRS}
  )


