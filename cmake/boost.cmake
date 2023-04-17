if (Boost_FOUND)
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

set(INSTALL_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/.boost/boost-lib)
SET(Boost_INCLUDE_DIRS "${INSTALL_LOCATION}/include")
SET(Boost_LIBRARY_DIRS "${INSTALL_LOCATION}/lib")
LINK_DIRECTORIES("${Boost_LIBRARY_DIRS}")

set(_cxxargs "-fPIC -DBOOST_NO_AUTO_PTR -fvisibility=default")
set(_b2args cxxflags=${_cxxargs};cflags=-fPIC;threading=multi; toolset=${_toolset} --without-python;--prefix=${INSTALL_LOCATION})

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
  URL https://boostorg.jfrog.io/artifactory/main/release/1.82.0/source/boost_1_82_0.tar.bz2
  URL_HASH SHA256=a6e1ab9b0860e6a2881dd7b21fe9f737a095e5f33a3a874afc6a345228597ee6
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
