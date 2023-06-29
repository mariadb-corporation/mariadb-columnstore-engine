

# Generate "something" to trigger cmake rerun when VERSION changes
CONFIGURE_FILE(
    ${ENGINE_SRC_DIR}/VERSION
  ${CMAKE_BINARY_DIR}/VERSION.dep
)

# Read value for a variable from VERSION.

MACRO(COLUMNSTORE_GET_CONFIG_VALUE keyword var)
 IF(NOT ${var})
     FILE (STRINGS ${ENGINE_SRC_DIR}/VERSION str REGEX "^[ ]*${keyword}=")
   IF(str)
     STRING(REPLACE "${keyword}=" "" str ${str})
     STRING(REGEX REPLACE  "[ ].*" ""  str "${str}")
     SET(${var} ${str})
   ENDIF()
 ENDIF()
ENDMACRO()

MACRO(GET_COLUMNSTORE_VERSION)
  COLUMNSTORE_GET_CONFIG_VALUE("COLUMNSTORE_VERSION_MAJOR" CS_MAJOR_VERSION)
  COLUMNSTORE_GET_CONFIG_VALUE("COLUMNSTORE_VERSION_MINOR" CS_MINOR_VERSION)
  COLUMNSTORE_GET_CONFIG_VALUE("COLUMNSTORE_VERSION_PATCH" CS_PATCH_VERSION)
  COLUMNSTORE_GET_CONFIG_VALUE("COLUMNSTORE_VERSION_EXTRA" CS_EXTRA_VERSION)
  COLUMNSTORE_GET_CONFIG_VALUE("COLUMNSTORE_VERSION_RELEASE" CS_RELEASE_VERSION)
  IF(CMAPI)
    SET(CMAPI_VERSION_MAJOR ${CS_MAJOR_VERSION})
    SET(CMAPI_VERSION_MINOR ${CS_MINOR_VERSION})
    SET(CMAPI_VERSION_PATCH ${CS_PATCH_VERSION}${CS_EXTRA_VERSION})
  ENDIF()

IF(NOT "${CS_MAJOR_VERSION}" MATCHES "[0-9]+" OR
   NOT "${CS_MINOR_VERSION}" MATCHES "[0-9]+" OR
   NOT "${CS_PATCH_VERSION}" MATCHES "[0-9]+")
    MESSAGE(FATAL_ERROR "VERSION file cannot be parsed.")
  ENDIF()

  SET(VERSION "${CS_MAJOR_VERSION}.${CS_MINOR_VERSION}.${CS_PATCH_VERSION}${CS_EXTRA_VERSION}")
  MESSAGE("== MariaDB-Columnstore ${VERSION}")
  IF (NOT INSTALL_LAYOUT)
      SET(CPACK_PACKAGE_VERSION_MAJOR ${CS_MAJOR_VERSION})
      SET(CPACK_PACKAGE_VERSION_MINOR ${CS_MINOR_VERSION})
      SET(CPACK_PACKAGE_VERSION_PATCH ${CS_PATCH_VERSION}${CS_EXTRA_VERSION})
  ENDIF ()
  SET(PACKAGE_VERSION "${CS_MAJOR_VERSION}.${CS_MINOR_VERSION}.${CS_PATCH_VERSION}${CS_EXTRA_VERSION}")
  SET(PACKAGE_RELEASE "${CS_RELEASE_VERSION}")
  MATH(EXPR MCSVERSIONHEX "${CS_MAJOR_VERSION} * 256 + ${CS_MINOR_VERSION}" OUTPUT_FORMAT HEXADECIMAL)

ENDMACRO()

# Get columnstore version
GET_COLUMNSTORE_VERSION()
