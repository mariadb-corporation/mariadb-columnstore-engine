include_directories(${ENGINE_COMMON_INCLUDES} ${S3API_DIR})

link_directories(${CMAKE_BINARY_DIR}/lib)

if(NOT PARQUET_ENABLED OR NOT ARROW_ENABLED)
    message(
        FATAL_ERROR
            "ColumnStore bulk (we_bulk/cpimport) requires Arrow and Parquet. "
            "Enable WITH_ARROW and configure WITH_PARQUET (bundled/system/auto)."
    )
endif()

# ########## next target ###############

set(we_bulk_STAT_SRCS
    we_brmreporter.cpp
    we_bulkload.cpp
    we_bulkloadbuffer.cpp
    we_bulkstatus.cpp
    we_cmdargs.cpp
    we_colopbulk.cpp
    we_colbuf.cpp
    we_colbufcompressed.cpp
    we_colbufmgr.cpp
    we_colbufmgrdctnry.cpp
    we_colbufsec.cpp
    we_colextinf.cpp
    we_columninfo.cpp
    we_columninfocompressed.cpp
    we_columnautoinc.cpp
    we_extentstripealloc.cpp
    we_tableinfo.cpp
    we_tempxmlgendata.cpp
    we_workers.cpp
)
list(APPEND we_bulk_STAT_SRCS we_parquet_reader.cpp)

add_definitions(-D_FILE_OFFSET_BITS=64)

columnstore_static_library(we_bulk ${we_bulk_STAT_SRCS})
columnstore_link(we_bulk ${NETSNMP_LIBRARIES} loggingcpp boost_program_options)
# Arrow / Parquet public headers emit -Wunused-parameter and (with GCC)
# -Wmissing-braces when parent targets use strict warnings and -Werror.
# Apply to GCC/Clang/Intel etc.; MSVC rejects unknown warning switches.
if(NOT MSVC)
    set_source_files_properties(
        "${CMAKE_CURRENT_SOURCE_DIR}/we_parquet_reader.cpp"
        PROPERTIES COMPILE_OPTIONS "-Wno-unused-parameter;-Wno-missing-braces"
    )
endif()
target_compile_definitions(we_bulk PRIVATE WITH_ARROW=1)
target_include_directories(we_bulk PRIVATE ${PARQUET_INCLUDE_DIRS})
columnstore_link(we_bulk ${PARQUET_LIBRARIES})
if(PARQUET_EXTERNAL_TARGET)
    add_dependencies(we_bulk ${PARQUET_EXTERNAL_TARGET})
endif()
target_include_directories(we_bulk PRIVATE ${ARROW_INCLUDE_DIRS})
columnstore_link(we_bulk ${ARROW_LIBRARIES})
if(ARROW_EXTERNAL_TARGET)
    add_dependencies(we_bulk ${ARROW_EXTERNAL_TARGET})
endif()

remove_definitions(-D_FILE_OFFSET_BITS=64)

# ########## next target ###############

set(cpimport.bin_SRCS cpimport.cpp)

columnstore_executable(cpimport.bin ${cpimport.bin_SRCS})
add_dependencies(cpimport.bin marias3)
columnstore_link(
    cpimport.bin
    ${ENGINE_LDFLAGS}
    ${NETSNMP_LIBRARIES}
    ${ENGINE_WRITE_LIBS}
    ${S3API_DEPS}
    we_bulk
    we_xml
)
target_compile_definitions(cpimport.bin PRIVATE WITH_ARROW=1)
target_include_directories(cpimport.bin PRIVATE ${PARQUET_INCLUDE_DIRS})
columnstore_link(cpimport.bin ${PARQUET_LIBRARIES})
if(PARQUET_EXTERNAL_TARGET)
    add_dependencies(cpimport.bin ${PARQUET_EXTERNAL_TARGET})
endif()
target_include_directories(cpimport.bin PRIVATE ${ARROW_INCLUDE_DIRS})
columnstore_link(cpimport.bin ${ARROW_LIBRARIES})
if(ARROW_EXTERNAL_TARGET)
    add_dependencies(cpimport.bin ${ARROW_EXTERNAL_TARGET})
endif()
