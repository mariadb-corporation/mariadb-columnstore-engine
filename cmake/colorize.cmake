
include_guard(GLOBAL)


find_program(TPUT NAMES tput tput.exe)
set(_term $ENV{TERM})
CMAKE_DEPENDENT_OPTION(COLORIZE_OUTPUT "Use console colors" TRUE "TPUT;_term;CMAKE_COLOR_MAKEFILE" FALSE)
unset(_term)

set(Reset   "")
set(Red     "")
set(Green   "")
set(Yellow  "")
set(Blue    "")
set(Magenta "")
set(Cyan    "")
set(White   "")  


if(COLORIZE_OUTPUT)
  execute_process(COMMAND ${TPUT} sgr0 OUTPUT_VARIABLE Reset)
  execute_process(COMMAND ${TPUT} setaf 1 OUTPUT_VARIABLE Red)
  execute_process(COMMAND ${TPUT} setaf 2 OUTPUT_VARIABLE Green)
  execute_process(COMMAND ${TPUT} setaf 3 OUTPUT_VARIABLE Yellow)
  execute_process(COMMAND ${TPUT} setaf 4 OUTPUT_VARIABLE Blue)
  execute_process(COMMAND ${TPUT} setaf 5 OUTPUT_VARIABLE Magenta)
  execute_process(COMMAND ${TPUT} setaf 6 OUTPUT_VARIABLE Cyan)
  execute_process(COMMAND ${TPUT} setaf 7 OUTPUT_VARIABLE White)
endif()

macro(status)
  MESSAGE(STATUS "${Cyan}${ARGN}${Reset}")
endmacro()

macro(warning)
  MESSAGE(WARNING "${Yellow}${ARGN}${Reset}")
endmacro()

macro(error)
  MESSAGE(FATAL_ERROR "${Red}${ARGN}${Reset}")
endmacro()



macro(dump)
  foreach(_item ${ARGV})
    MESSAGE(STATUS "${Magenta}${_item}${Reset}: ${Green}${${_item}}${Reset}")
  endforeach()
endmacro()

macro(dump_all_variables)
  get_cmake_property(_variableNames VARIABLES)
  list (SORT _variableNames)
  foreach (_variableName ${_variableNames})
    if("Reset" STREQUAL "${_variableName}" OR
      "Red" STREQUAL "${_variableName}" OR
      "Green" STREQUAL "${_variableName}" OR
      "Yellow" STREQUAL "${_variableName}" OR
      "Blue" STREQUAL "${_variableName}" OR
      "Magenta" STREQUAL "${_variableName}" OR
      "Cyan" STREQUAL "${_variableName}" OR
      "White" STREQUAL "${_variableName}")
        continue()
    endif()
    dump(${_variableName})
  endforeach()
  unset(_variableNames)
endmacro()