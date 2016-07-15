
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(dataconvert_LIB_SRCS dataconvert.cpp)

add_library(dataconvert SHARED ${dataconvert_LIB_SRCS})

set_target_properties(dataconvert PROPERTIES VERSION 1.0.0 SOVERSION 1)

<<<<<<< HEAD
install(TARGETS dataconvert DESTINATION ${ENGINE_LIBDIR})
=======
#install(TARGETS dataconvert DESTINATION ${INSTALL_TARGETS_DEFAULT_ARGS})
>>>>>>> 6bbb56b0091c6f90b74049e682009f3d1f57d45e

