target_include_directories(we_xml_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(we_xml STATIC 
    we_xmlop.cpp
    we_xmljob.cpp
    we_xmlgendata.cpp
    we_xmlgenproc.cpp)

target_compile_definitions(we_xml PRIVATE _FILE_OFFSET_BITS=64)

target_link_libraries(we_xml 
  INTERFACE we_xml_h
  PRIVATE     
    LibXml2::LibXml2
    execplan_h
    messageqcpp_h
    joblist_h
    idbdatafile_h
    dataconvert_h
    loggingcpp_h
    we_shared_h
    )

INSTALL(TARGETS we_xml DESTINATION ${ENGINE_LIBDIR})

