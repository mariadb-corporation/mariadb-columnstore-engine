# Function to create either a static or shared library based on COLUMNSTORE_STATIC_LIBRARIES
macro(columnstore_library libname)
    if(COLUMNSTORE_STATIC_LIBRARIES)
        add_library(${libname} STATIC ${ARGN})
    else()
        add_library(${libname} SHARED ${ARGN})
    endif()
endmacro()
