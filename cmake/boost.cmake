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
  URL https://boostorg.jfrog.io/artifactory/main/release/1.79.0/source/boost_1_79_0.tar.bz2
  URL_HASH SHA256=475d589d51a7f8b3ba2ba4eda022b170e562ca3b760ee922c146b6c65856ef39
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
