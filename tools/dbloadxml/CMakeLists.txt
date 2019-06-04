
# dbload

add_library(dbload STATIC inputmgr.cpp)

target_link_libraries(dbload 
  PRIVATE 
    execplan_h
    joblist_h
    messageqcpp_h
    we_xml_h
    we_shared_h
  )


# colxml

add_executable(colxml colxml.cpp)

target_link_libraries(colxml 
  PRIVATE 
    Boost::filesystem
    brm
    common_h
    configcpp
    dbload
    execplan
    idbdatafile_h
    joblist_h
    loggingcpp
    messageqcpp_h
    rwlock_h
    we_xml
    writeengine
    we_shared_h
    LibXml2::LibXml2
  )

install(TARGETS colxml DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

