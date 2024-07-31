CMake Super Build
=================
A super build is a process to download, build and install dependencies with cmake at configure time. This ensure dependencies are available during the initial configure stage. Its accomplished by executing separate cmake configure and build processes inline with the main project cmake which builds and installs the missing dependency.

Rationale:
----------
It maybe observed that ExternalProject accomplishes a similar task, however, the target of an ExternalProject is not available until after the build stage. Any scripting logic which requires the dependency during the configure stage will fail. The super build solves this by ensuring the dependency is built independent of the main projects configuration which uses it.

Example:
--------
# In the context of the main projects cmake scripts, subshells of cmake are executed to configure and build the dependency
configure_file(some_dependency.CMakeLists.txt.in some_dep_dir\CMakeLists.txt @ONLY) # drop a top-level CMakeLists.txt in a folder for the dependency
execute_process(COMMAND ${CMAKE_COMMAND} . WORKING_DIRECTORY some_dep_dir) # execute configure stage of dependency against newly created CMakeLists.txt from above step
execute_process(COMMAND ${CMAKE_COMMAND} --build . WORKING_DIRECTORY some_dep_dir) # install the dependency
find_package(some_dependency) # the dependency should be installed and can be 'found' or used as appropriate

NOTES
-----
 o The bulk of the work is performed in the generated/copied CMakeLists.txt to download (optional), build and install the dependency. It typically contains the full set of ExternalProject statements and error handling.
 o CMake scripts executed in a sub-process with execute_process are independent and share no state whatsoever with the calling process. There are two ways to share state with the sub-shell
   - Wrap appropriate @VARIABLE@ decorations in the CMakeLists.in template which get substituted with values when configure_file is executed
   - Pass them on the command line of the execute_process statement. e.g.: execute_process(COMMAND ${CMAKE_COMMAND} -DSOME_VAR=${SOME_VAL} -DANOTHER_VAR=${ANOTHER_VAL} ...

x