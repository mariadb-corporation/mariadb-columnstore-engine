# cmake module to enable use of libre2.

OPTION(USE_LIBRE2 "Enable re2 as an external project" ON)

IF(USE_LIBRE2)

  EXTERNALPROJECT_ADD(libre2
                       PREFIX "libre2"
                       GIT_REPOSITORY "https://github.com/google/re2.git"
                       CMAKE_ARGS "-DBUILD_SHARED_LIBS=ON -DCMAKE_INSTALL_PREFIX=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/.."
                       INSTALL_COMMAND cp libre2.so ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libre2-mcs.so
                      )
  ADD_DEFINITIONS("-DWITH_LIBRE2")
  EXTERNALPROJECT_GET_PROPERTY(libre2 SOURCE_DIR)
  MESSAGE(STATUS "libre2 source dir: ${SOURCE_DIR}")
  INCLUDE_DIRECTORIES(libre2 "${SOURCE_DIR}")
  SET(LIBRE2_LIB "-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY}" "libre2-mcs.so")

ENDIF()

