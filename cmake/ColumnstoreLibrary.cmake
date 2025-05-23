# Function to create either a static or shared library based on COLUMNSTORE_STATIC_LIBRARIES
macro(columnstore_library libname)
    if(COLUMNSTORE_STATIC_LIBRARIES)
        add_definitions(-fPIC -DPIC)
        add_library(${libname} STATIC ${ARGN})
    else()
        add_library(${libname} SHARED ${ARGN})

        install(
            TARGETS ${libname}
            DESTINATION ${ENGINE_LIBDIR}
            COMPONENT columnstore-engine
        )
    endif()

endmacro()

macro(columnstore_link libname)
    target_link_libraries(${libname} ${ARGN})
endmacro()
