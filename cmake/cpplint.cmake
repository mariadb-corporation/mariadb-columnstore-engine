include_guard(GLOBAL)



option(USE_CPPLINT "Scan source with cpplint" FALSE)

if(NOT USE_CPPLINT)
  unset(CMAKE_CXX_CPPLINT)
  return()
endif()

if(NOT Python_FOUND)
  warning("Python not found. CPPLINT disabled")
  set(USE_CPPLINT FALSE)
  unset(CMAKE_CXX_CPPLINT)
  return()
endif()

file(DOWNLOAD https://raw.githubusercontent.com/cpplint/cpplint/master/cpplint.py ${CMAKE_CURRENT_BINARY_DIR}/cpplint.py
  EXPECTED_HASH MD5=389ac5707fad4b9e29cd4fe7aaff28c9)

set(CMAKE_CXX_CPPLINT ${Python_EXECUTABLE};${CMAKE_CURRENT_BINARY_DIR}/cpplint.py)

