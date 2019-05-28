
target_include_directories(we_shared_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


if(BUILD_TESTING)
  cs_add_executable(we_shared_components_tests ./shared_components_tests.cpp)
  install(TARGETS we_shared_components_tests DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)
endif()

install(FILES  we_index.h we_define.h we_type.h we_fileop.h we_blockop.h we_dbfileop.h we_obj.h we_log.h 
          we_simplesyslog.h we_convertor.h we_brm.h we_macro.h we_config.h we_cache.h we_stats.h 
          we_bulkrollbackmgr.h we_typeext.h we_chunkmanager.h we_bulkrollbackfilecompressed.h 
          we_bulkrollbackfilecompressedhdfs.h we_bulkrollbackfile.h we_rbmetawriter.h we_dbrootextenttracker.h 
          we_confirmhdfsdbfile.h 
        DESTINATION include)
