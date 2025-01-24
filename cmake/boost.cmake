if (Boost_FOUND)
  add_custom_target(external_boost)
  return()
endif()

include(ExternalProject)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  set(_toolset "gcc")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.0")
    set(_extra "pch=off")
  endif()
elseif(CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
  set(_toolset "clang")
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Intel")
  set(_toolset "intel-linux")
endif()

set(INSTALL_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/.boost/boost-lib)
SET(Boost_INCLUDE_DIRS "${INSTALL_LOCATION}/include")
SET(Boost_LIBRARY_DIRS "${INSTALL_LOCATION}/lib")
LINK_DIRECTORIES("${Boost_LIBRARY_DIRS}")

set(_cxxargs "-fPIC -DBOOST_NO_AUTO_PTR -fvisibility=default")
set(_b2args cxxflags=${_cxxargs};cflags=-fPIC;threading=multi;${_extra}; --without-python;--prefix=${INSTALL_LOCATION})

SET(byproducts)
FOREACH(name chrono filesystem program_options regex system thread)
  SET(lib boost_${name})
  ADD_LIBRARY(${lib} STATIC IMPORTED GLOBAL)
  ADD_DEPENDENCIES(${lib} external_boost)
  SET (loc "${Boost_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}${lib}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  SET(byproducts ${byproducts} BUILD_BYPRODUCTS ${loc})
  SET_TARGET_PROPERTIES(${lib} PROPERTIES IMPORTED_LOCATION ${loc}
                                          EXCLUDE_FROM_ALL TRUE)
ENDFOREACH()

ExternalProject_Add(external_boost
  PREFIX .boost
  URL https://archives.boost.io/release/1.84.0/source/boost_1_84_0.tar.gz
  URL_HASH SHA256=a5800f405508f5df8114558ca9855d2640a2de8f0445f051fa1c7c3383045724
  CONFIGURE_COMMAND ./bootstrap.sh
  UPDATE_COMMAND ""
  BUILD_COMMAND ./b2 -q ${_b2args}
  BUILD_IN_SOURCE TRUE
  INSTALL_COMMAND ./b2 -q install ${_b2args}
  LOG_BUILD TRUE
  LOG_INSTALL TRUE
  EXCLUDE_FROM_ALL TRUE
  ${byproducts}
)
