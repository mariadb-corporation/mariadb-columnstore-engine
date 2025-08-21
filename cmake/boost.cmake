find_package(Boost 1.88.0 COMPONENTS chrono filesystem program_options regex system thread)

if(Boost_FOUND)
    add_custom_target(external_boost)
    return()
endif()

include(ExternalProject)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    set(_toolset "gcc")
elseif(CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
    set(_toolset "clang")
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Intel")
    set(_toolset "intel-linux")
endif()

message("${_toolset} will be used for Boost compilation with compiler ${CMAKE_CXX_COMPILER_ID}")

set(INSTALL_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/.boost/boost-lib)
set(BOOST_ROOT "${INSTALL_LOCATION}")
set(Boost_INCLUDE_DIRS "${INSTALL_LOCATION}/include")
set(Boost_LIBRARY_DIRS "${INSTALL_LOCATION}/lib")
link_directories("${Boost_LIBRARY_DIRS}")

set(_cxxargs "-fPIC -DBOOST_NO_AUTO_PTR -fvisibility=default")
set(_linkflags "")

if(WITH_MSAN)
    set(_cxxargs "${_cxxargs} -fsanitize=memory -fsanitize-memory-track-origins -U_FORTIFY_SOURCE")
    set(_linkflags "-stdlib=libc++")
elseif(COLUMNSTORE_WITH_LIBCPP)
    set(_cxxargs "${_cxxargs} -stdlib=libc++")
    set(_linkflags "-stdlib=libc++")
endif()

set(_b2args cxxflags=${_cxxargs};cflags=-fPIC;threading=multi;${_extra};toolset=${_toolset}
            --without-mpi;--without-charconv;--without-python;--prefix=${INSTALL_LOCATION} linkflags=${_linkflags}
)

set(byproducts)
foreach(name chrono filesystem program_options regex system thread)
    set(lib boost_${name})
    add_library(${lib} STATIC IMPORTED GLOBAL)
    add_dependencies(${lib} external_boost)
    set(loc "${Boost_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}${lib}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(byproducts ${byproducts} BUILD_BYPRODUCTS ${loc})
    set_target_properties(${lib} PROPERTIES IMPORTED_LOCATION ${loc} EXCLUDE_FROM_ALL TRUE)
endforeach()

set(LOG_BOOST_INSTEAD_OF_SCREEN "")
if(COLUMNSTORE_MAINTAINER_MODE)
    set(LOG_BOOST_INSTEAD_OF_SCREEN "LOG_BUILD TRUE LOG_INSTALL TRUE")
endif()

ExternalProject_Add(
    external_boost
    PREFIX .boost
    URL https://archives.boost.io/release/1.88.0/source/boost_1_88_0.tar.gz
    URL_HASH SHA256=3621533e820dcab1e8012afd583c0c73cf0f77694952b81352bf38c1488f9cb4
    CONFIGURE_COMMAND ./bootstrap.sh
    UPDATE_COMMAND ""
    PATCH_COMMAND ${CMAKE_COMMAND} -E chdir <SOURCE_DIR> patch -p1 -i
                  ${CMAKE_SOURCE_DIR}/storage/columnstore/columnstore/cmake/boost.1.88.named_proxy.hpp.patch
    BUILD_COMMAND ./b2 -q ${_b2args}
    BUILD_IN_SOURCE TRUE
    INSTALL_COMMAND ./b2 -q install ${_b2args}
    ${LOG_BOOST_INSTEAD_OF_SCREEN}
    EXCLUDE_FROM_ALL TRUE
    ${byproducts}
)
