include(ExternalProject)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  set(_toolset "gcc")
elseif(CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
  set(_toolset "clang")
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Intel")
  set(_toolset "intel-linux")
endif()

ExternalProject_Add(mpark_patterns
  PREFIX external/patterns
  URL https://github.com/mpark/patterns/archive/refs/tags/v0.3.0.tar.gz
  URL_HASH SHA256=80e6af808a4d74d5d7358666303eb1dbfc5582313ff9fa31d1c0d3280d3bd9e7
  BUILD_IN_SOURCE TRUE
  LOG_BUILD TRUE
  LOG_INSTALL TRUE
  EXCLUDE_FROM_ALL TRUE
)
