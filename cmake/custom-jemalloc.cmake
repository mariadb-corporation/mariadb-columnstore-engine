if (WITH_CUSTOM_JEMALLOC)
    include(ExternalProject)

    set(INSTALL_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/external/jemalloc)
    SET(Jemalloc_INCLUDE_DIRS "${INSTALL_LOCATION}/include")
    SET(Jemalloc_LIBRARY_DIRS "${INSTALL_LOCATION}/lib")
    SET(LD_PRELOAD_STRING "")
    ADD_LIBRARY(custom_jemalloc_lib INTERFACE)
    ADD_DEPENDENCIES(custom_jemalloc_lib custom_jemalloc)
    set(JEMALLOC_LIB ${Jemalloc_LIBRARY_DIRS}/${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc_pic${CMAKE_STATIC_LIBRARY_SUFFIX})

    # the --no-as-needed and -force_load make the linker include all symbols,
    # including ones that appear unused - this is essential when using jemalloc
    # as a static library: it uses `__attribute__((constructor))` to do very early
    # process-level initialization, and some linkers will omit those functions
    # when linking a static library as there are no explicit callsites.
    message ("JemAlloc custom build static library file ${JEMALLOC_LIB}")
    target_link_libraries(custom_jemalloc_lib INTERFACE "-Wl,--no-as-needed" "${JEMALLOC_LIB}" "-Wl,--as-needed")
    target_link_libraries(custom_jemalloc_lib INTERFACE dl)

    ExternalProject_Add(custom_jemalloc
        URL https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2
        URL_HASH SHA256=2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa
        CONFIGURE_COMMAND ./configure --with-jemalloc-prefix="" --enable-shared --enable-prof --enable-static --prefix=${INSTALL_LOCATION}

        UPDATE_COMMAND ""
        BUILD_COMMAND make
        BUILD_IN_SOURCE TRUE
        INSTALL_COMMAND make install
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
        EXCLUDE_FROM_ALL TRUE
    )
endif()
