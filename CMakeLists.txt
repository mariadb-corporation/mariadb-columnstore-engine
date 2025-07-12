cmake_minimum_required(VERSION 3.13)

project(Columnstore)

foreach(p CMP0135 CMP0077)
    if(POLICY ${p})
        cmake_policy(SET ${p} NEW)
    endif()
endforeach()

set(PACKAGE columnstore)
set(PACKAGE_NAME columnstore)
set(PACKAGE_TARNAME columnstore)
set(PACKAGE_BUGREPORT support@mariadb.com)
set(PACKAGE_URL "")
set(PACKAGE_STRING columnstore-${PACKAGE_VERSION})

set_property(DIRECTORY PROPERTY EP_BASE ${CMAKE_CURRENT_BINARY_DIR}/external)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/cmake)

set(CMAKE_CXX_EXTENSIONS FALSE)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/obj)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)

# take a note, that install scripts are and services configurations are located oam/install_scripts/CMakeLists.txt

set(WITH_COLUMNSTORE_LZ4
    AUTO
    CACHE STRING "Build with lz4. Possible values are 'ON', 'OFF', 'AUTO' and default is 'AUTO'"
)

# order of includes is important
include(check_compiler_flag)
include(ExternalProject)
include(CheckCXXSourceCompiles)
include(packages)
include(boost)
include(thrift)
include(dirs)
include(includes)
include(libs)
include(ColumnstoreLibrary)
include(columnstore_version)
include(configureEngine)
include(compiler_flags)
include(misc)
include(cpack_manage)

add_subdirectory(dbcon/mysql)
if(NOT TARGET columnstore)
    return()
endif()

set(COMPONENTS
    utils
    oam/oamcpp
    dbcon/execplan
    dbcon/joblist
    versioning
    oam
    writeengine/wrapper
    writeengine/client
    writeengine/xml
    writeengine/redistribute
    dbcon/ddlpackage
    dbcon/ddlpackageproc
    dbcon/dmlpackage
    dbcon/dmlpackageproc
    ddlproc
    dmlproc
    oamapps
    primitives
    tools
    writeengine/server
    writeengine/bulk
    writeengine/splitter
    storage-manager
    datatypes
    tests
)

foreach(component ${COMPONENTS})
    add_subdirectory(${component})
endforeach()

add_dependencies(udf_mysql GenError)
add_dependencies(funcexp GenError)
add_dependencies(oamcpp GenError)
add_dependencies(regr_mysql GenError)
