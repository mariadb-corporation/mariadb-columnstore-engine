set(WITH_THRIFT
    "bundled"
    CACHE STRING "Which Thrift to use (possible values are 'bundled', 'system', or 'auto')"
)

if(WITH_THRIFT STREQUAL "system" OR WITH_THRIFT STREQUAL "auto")
    find_package(Thrift)

    if(Thrift_FOUND)
        add_custom_target(external_thrift)
        set(THRIFT_INCLUDE_DIR "${THRIFT_INCLUDE_DIR}")
        set(THRIFT_LIBRARY "${THRIFT_LIBRARIES}")
        return()
    elseif(WITH_THRIFT STREQUAL "system")
        FIND_PACKAGE(PkgConfig REQUIRED)
        pkg_check_modules(THRIFT REQUIRED thrift)
        
        if(THRIFT_FOUND)
            add_custom_target(external_thrift)
            set(THRIFT_INCLUDE_DIR "${THRIFT_INCLUDE_DIR}")
            set(THRIFT_LIBRARY "${THRIFT_LIBRARIES}")
            return()
        else()
            message(FATAL_ERROR "System Thrift requested but not found!")
        endif()
    endif()
endif()

include(ExternalProject)

set(INSTALL_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/external/thrift)
set(THRIFT_INCLUDE_DIRS "${INSTALL_LOCATION}/include")
set(THRIFT_LIBRARY_DIRS "${INSTALL_LOCATION}/lib")
set(THRIFT_LIBRARY ${THRIFT_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}thrift${CMAKE_STATIC_LIBRARY_SUFFIX})

set(cxxflags -fPIC)

set(linkflags "")
if(WITH_MSAN)
    set(cxxflags "'${cxxflags} -fsanitize=memory -fsanitize-memory-track-origins -U_FORTIFY_SOURCE -stdlib=libc++'")
    set(linkflags "'${linkflags} -stdlib=libc++'")
elseif(COLUMNSTORE_WITH_LIBCPP)
    set(cxxflags "'${cxxflags} -stdlib=libc++'")
    set(linkflags "'${linkflags} -stdlib=libc++'")
endif()

ExternalProject_Add(
    external_thrift
    URL https://github.com/apache/thrift/archive/refs/tags/v0.22.0.tar.gz
    URL_HASH SHA256=c4649c5879dd56c88f1e7a1c03e0fbfcc3b2a2872fb81616bffba5aa8a225a37
    PREFIX ${INSTALL_LOCATION}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${INSTALL_LOCATION}
               -DBUILD_COMPILER=YES
               -DBUILD_CPP=YES
               -DBUILD_C_GLIB=YES
               -DBUILD_JAVA=NO
               -DBUILD_JAVASCRIPT=NO
               -DBUILD_KOTLIN=NO
               -DBUILD_NODEJS=NO
               -DBUILD_PYTHON=NO
               -DBUILD_TESTING=NO
               -DBUILD_SHARED_LIBS=NO
               -DBUILD_TUTORIALS=NO
               -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
               -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
               -DCMAKE_CXX_FLAGS:STRING=${cxxflags}
               -DBOOST_ROOT=${BOOST_ROOT}
               -DCMAKE_EXE_LINKER_FLAGS=${linkflags}
               -DCMAKE_SHARED_LINKER_FLAGS=${linkflags}
               -DCMAKE_MODULE_LINKER_FLAGS=${linkflags}
    BUILD_BYPRODUCTS "${THRIFT_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}thrift${CMAKE_STATIC_LIBRARY_SUFFIX}"
    EXCLUDE_FROM_ALL TRUE
)

add_dependencies(external_thrift external_boost)
