
# dbload

add_library(dbload STATIC inputmgr.cpp)

target_link_libraries(dbload 
  PRIVATE execplan_h
          joblist_h
          messageqcpp_h
          we_shared_h          
          we_xml_h)


# colxml

add_executable(colxml colxml.cpp)

target_link_libraries(colxml 
  PRIVATE brm_h
          common_h
          configcpp_h
          execplan_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          we_xml_h
          we_shared_h
          LibXml2::LibXml2)

install(TARGETS colxml DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

