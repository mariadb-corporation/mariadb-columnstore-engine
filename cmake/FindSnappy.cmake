# - Try to find snappy headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(Snappy)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  SNAPPY_ROOT_DIR Set this variable to the root installation of
#                    jemalloc if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  SNAPPY_FOUND             System has snappy libs/headers
#  SNAPPY_LIBRARIES         The snappy library/libraries
#  SNAPPY_INCLUDE_DIR       The location of snappy headers

find_path(SNAPPY_ROOT_DIR
    NAMES include/snappy.h
)

find_library(SNAPPY_LIBRARIES
    NAMES snappy
    HINTS ${SNAPPY_ROOT_DIR}/lib64 ${SNAPPY_ROOT_DIR}/lib
)

find_path(SNAPPY_INCLUDE_DIR
    NAMES snappy.h
    HINTS ${JEMALLOC_ROOT_DIR}/include
)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(Snappy
    REQUIRED_VARS SNAPPY_LIBRARIES SNAPPY_INCLUDE_DIR
)

mark_as_advanced(
    SNAPPY_ROOT_DIR
    SNAPPY_LIBRARIES
    SNAPPY_INCLUDE_DIR
)

add_library(Snappy::Snappy UNKNOWN IMPORTED)

set_target_properties(Snappy::Snappy PROPERTIES 
  IMPORTED_LOCATION ${SNAPPY_LIBRARIES}
  IMPORTED_LINK_INTERFACE_LANGUAGES "C"
  INTERFACE_INCLUDE_DIRECTORIES ${SNAPPY_INCLUDE_DIR}
  )
