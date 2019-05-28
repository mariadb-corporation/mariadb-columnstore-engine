

set(_super_build_cmakelists_template ${CMAKE_CURRENT_LIST_DIR}/superbuild.CMakeLists.txt.in)

function(superbuild _target)

  set(_superbuild_dir ${CMAKE_CURRENT_BINARY_DIR}/.${_target})

  configure_file(${_super_build_cmakelists_template}  ${_superbuild_dir}/CMakeLists.txt @ONLY)

  status("${Cyan}Configuring ${_target}. Please Wait.")

  execute_process(
    COMMAND ${CMAKE_COMMAND} .
    WORKING_DIRECTORY ${_superbuild_dir}
    RESULT_VARIABLE _superbuild_ret
    OUTPUT_VARIABLE _superbuild_stdout
    ERROR_VARIABLE _superbuild_stderr
  )

  if(_superbuild_ret AND _superbuild_stderr)
    error(${_superbuild_stderr})
  endif()

  status(${_superbuild_stdout})

  status("${Cyan}Building ${_target}. Please Wait.")

  unset(_superbuild_ret _superbuild_stderr _superbuild_stdout)
  execute_process(
    COMMAND ${CMAKE_COMMAND} --build . -j ${BUILD_THREADS_MAX}
    WORKING_DIRECTORY ${_superbuild_dir}
    RESULT_VARIABLE _superbuild_ret
    OUTPUT_VARIABLE _superbuild_stdout
    ERROR_VARIABLE _superbuild_stderr
  )

  if(_superbuild_ret AND _superbuild_stderr)
    error(${_superbuild_stderr})
  endif()

  status(${_superbuild_stdout})
  unset(_superbuild_ret _superbuild_stderr _superbuild_stdout)

endfunction()