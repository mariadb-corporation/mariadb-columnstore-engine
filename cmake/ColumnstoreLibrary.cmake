set(DEBIAN_INSTALL_FILE "${CMAKE_SOURCE_DIR}/storage/columnstore/columnstore/debian/mariadb-plugin-columnstore.install")

file(WRITE ${DEBIAN_INSTALL_FILE} )

macro(add_to_debian_install_file file_path)
    string(SUBSTRING "${file_path}\n" 1 -1 BINARY_ENTRY)
    file(APPEND ${DEBIAN_INSTALL_FILE} "${BINARY_ENTRY}")
endmacro()

macro(columnstore_install_target target destination)
    install(
        TARGETS ${target}
        DESTINATION ${destination}
        COMPONENT columnstore-engine
    )
    add_to_debian_install_file("${destination}/${target}")
endmacro()

macro(columnstore_install_file file destination)
    install(
        FILES ${file}
        DESTINATION ${destination}
        COMPONENT columnstore-engine
    )
    get_filename_component(FILENAME ${file} NAME)
    add_to_debian_install_file("${destination}/${FILENAME}")
endmacro()

macro(columnstore_install_program file destination)
    install(
        PROGRAMS ${file}
        DESTINATION ${destination}
        COMPONENT columnstore-engine
    )
    get_filename_component(FILENAME ${file} NAME)
    add_to_debian_install_file("${destination}/${FILENAME}")
endmacro()

macro(columnstore_library libname)
    if(COLUMNSTORE_STATIC_LIBRARIES)
        add_definitions(-fPIC -DPIC)
        add_library(${libname} STATIC ${ARGN})
    else()
        add_library(${libname} SHARED ${ARGN})
        columnstore_install_target(${libname} ${ENGINE_LIBDIR})
    endif()
endmacro()

macro(columnstore_link libname)
    target_link_libraries(${libname} ${ARGN})
endmacro()

macro(columnstore_executable executable_name)
    add_executable(${executable_name} ${ARGN})
    columnstore_install_target(${executable_name} ${ENGINE_BINDIR})
endmacro()
