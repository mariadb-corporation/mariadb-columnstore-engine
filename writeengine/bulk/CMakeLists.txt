include_directories(${ENGINE_COMMON_INCLUDES} ${S3API_DIR})

link_directories(${CMAKE_BINARY_DIR}/lib)

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

add_definitions(-D_FILE_OFFSET_BITS=64)

columnstore_static_library(we_bulk ${we_bulk_STAT_SRCS})
columnstore_link(we_bulk loggingcpp boost_program_options)

remove_definitions(-D_FILE_OFFSET_BITS=64)

# ########## next target ###############

set(cpimport.bin_SRCS cpimport.cpp)

columnstore_executable(cpimport.bin ${cpimport.bin_SRCS})

columnstore_link(
    cpimport.bin
    ${ENGINE_LDFLAGS}
    ${ENGINE_WRITE_LIBS}
    ${S3API_DEPS}
    we_bulk
    we_xml
    marias3
)
