find_program(TPUT NAMES tput tput.exe)

set(Reset   "")
set(Red     "")
set(Green   "")
set(Yellow  "")
set(Blue    "")
set(Magenta "")
set(Cyan    "")
set(White   "")  

if(TPUT)
  execute_process(COMMAND ${TPUT} sgr0 OUTPUT_VARIABLE Reset)
  execute_process(COMMAND ${TPUT} setaf 1 OUTPUT_VARIABLE Red)
  execute_process(COMMAND ${TPUT} setaf 2 OUTPUT_VARIABLE Green)
  execute_process(COMMAND ${TPUT} setaf 3 OUTPUT_VARIABLE Yellow)
  execute_process(COMMAND ${TPUT} setaf 4 OUTPUT_VARIABLE Blue)
  execute_process(COMMAND ${TPUT} setaf 5 OUTPUT_VARIABLE Magenta)
  execute_process(COMMAND ${TPUT} setaf 6 OUTPUT_VARIABLE Cyan)
  execute_process(COMMAND ${TPUT} setaf 7 OUTPUT_VARIABLE White)
endif()

function(status)
  MESSAGE(STATUS "${Cyan}${ARGN}${Reset}")
endfunction()

function(warning)
  MESSAGE(WARNING "${Yellow}${ARGN}${Reset}")
endfunction()

function(error)
  MESSAGE(FATAL_ERROR "${Red}${ARGN}${Reset}")
endfunction()

function(dump_all_variables)
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
    message(STATUS "${Magenta}${_variableName}${Reset}=${Green}${${_variableName}}${Reset}")
  endforeach()
  unset(_variableNames)
endfunction()