
include_directories( ${ENGINE_COMMON_INCLUDES} ${S3API_DIR} )

link_directories(${CMAKE_BINARY_DIR}/lib)

########### next target ###############

set(we_bulk_STAT_SRCS
    we_brmreporter.cpp
    we_bulkload.cpp
    we_bulkloadbuffer.cpp
    we_bulkstatus.cpp
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
    we_workers.cpp)

ADD_DEFINITIONS(-D_FILE_OFFSET_BITS=64)
add_library(we_bulk STATIC ${we_bulk_STAT_SRCS})

add_dependencies(we_bulk loggingcpp)

target_link_libraries(we_bulk ${NETSNMP_LIBRARIES})

REMOVE_DEFINITIONS(-D_FILE_OFFSET_BITS=64)

########### next target ###############

set(cpimport.bin_SRCS cpimport.cpp)

add_executable(cpimport.bin ${cpimport.bin_SRCS})
add_dependencies(cpimport.bin marias3)
target_link_libraries(cpimport.bin ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${S3API_DEPS} we_bulk we_xml)

install(TARGETS cpimport.bin DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

