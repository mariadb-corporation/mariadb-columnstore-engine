
include_directories(${ENGINE_COMMON_INCLUDES} ../dictionary)

add_executable(we_shared_components_tests ./shared_components_tests.cpp)

add_dependencies(we_shared_components_tests loggingcpp)

target_link_libraries(we_shared_components_tests ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES})

install(TARGETS we_shared_components_tests DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

########### install files ###############

install(FILES  we_index.h we_define.h we_type.h we_fileop.h we_blockop.h we_dbfileop.h we_obj.h we_log.h we_simplesyslog.h we_convertor.h we_brm.h we_macro.h we_config.h we_cache.h we_stats.h we_bulkrollbackmgr.h we_typeext.h we_chunkmanager.h we_bulkrollbackfilecompressed.h we_bulkrollbackfilecompressedhdfs.h we_bulkrollbackfile.h we_rbmetawriter.h we_dbrootextenttracker.h we_confirmhdfsdbfile.h DESTINATION include)
