
target_include_directories(idbdatafile_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


add_library(idbdatafile SHARED 
    BufferedFile.cpp
    IDBDataFile.cpp
    IDBFactory.cpp
    IDBFileSystem.cpp
    IDBLogger.cpp
    IDBPolicy.cpp
    PosixFileSystem.cpp
    UnbufferedFile.cpp)

target_link_libraries(idbdatafile 
  PUBLIC idbdatafile_h 
  PRIVATE alarmmanager_h
          brm_h
          common_h
          compress_h
          configcpp_h
          idbboot_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h )

target_compile_definitions(idbdatafile PRIVATE BOOST_NO_CXX11_SCOPED_ENUMS)

target_pch(idbdatafile)

set_target_properties(idbdatafile PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS idbdatafile DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)
