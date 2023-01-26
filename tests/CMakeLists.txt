include_directories( ${ENGINE_COMMON_INCLUDES} ${ENGINE_BLOCKCACHE_INCLUDE} ${ENGINE_PRIMPROC_INCLUDE} )


#
# ag CPPUNIT_TEST_SUITE -l | grep -v tests/  > tests.txt
# cat tests.txt | awk '{source=$0; path=$0; gsub("/", "_", source); gsub(".cpp", "", source);  print "add_executable(" source " ../" path  ")\ntarget_link_libraries(" source " ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)\nadd_test(NAME columnstore:" source " COMMAND "source ")\n"}' > tests.cmd

add_executable(utils_dataconvert_tdriver ../utils/dataconvert/tdriver.cpp)
target_link_libraries(utils_dataconvert_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:utils_dataconvert_tdriver COMMAND utils_dataconvert_tdriver)

add_executable(utils_threadpool_tdriver ../utils/threadpool/tdriver.cpp)
target_link_libraries(utils_threadpool_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:utils_threadpool_tdriver COMMAND utils_threadpool_tdriver)

add_executable(utils_funcexp_tdriver ../utils/funcexp/tdriver.cpp)
target_link_libraries(utils_funcexp_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:utils_funcexp_tdriver COMMAND utils_funcexp_tdriver)

add_executable(utils_confi_tdriver ../utils/configcpp/tdriver.cpp)
target_link_libraries(utils_confi_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:utils_confi_tdriver COMMAND utils_confi_tdriver)

add_executable(utils_confi_tdriver2 ../utils/configcpp/tdriver2.cpp)
target_link_libraries(utils_confi_tdriver2 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:utils_confi_tdriver2 COMMAND utils_confi_tdriver2)

add_executable(utils_rwlock_tdriver ../utils/rwlock/tdriver.cpp)
target_link_libraries(utils_rwlock_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:utils_rwlock_tdriver COMMAND utils_rwlock_tdriver)

add_executable(utils_rwlock_tdriver-rw ../utils/rwlock/tdriver-rw.cpp)
target_link_libraries(utils_rwlock_tdriver-rw ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:utils_rwlock_tdriver-rw COMMAND utils_rwlock_tdriver-rw)

add_executable(utils_loggin_tdriver ../utils/loggingcpp/tdriver.cpp)
target_link_libraries(utils_loggin_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:utils_loggin_tdriver COMMAND utils_loggin_tdriver)

add_executable(utils_message_tdriver ../utils/messageqcpp/tdriver.cpp)
target_link_libraries(utils_message_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:utils_message_tdriver COMMAND utils_message_tdriver)

add_executable(dbcon_ddlpackageproc_tdriver ../dbcon/ddlpackageproc/tdriver.cpp)
target_link_libraries(dbcon_ddlpackageproc_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_ddlpackageproc_tdriver COMMAND dbcon_ddlpackageproc_tdriver)

add_executable(dbcon_ddlpackage_tdriver ../dbcon/ddlpackage/tdriver.cpp)
target_link_libraries(dbcon_ddlpackage_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_ddlpackage_tdriver COMMAND dbcon_ddlpackage_tdriver)

add_executable(dbcon_ddlpackage_old-tdriver ../dbcon/ddlpackage/old-tdriver.cpp)
target_link_libraries(dbcon_ddlpackage_old-tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_ddlpackage_old-tdriver COMMAND dbcon_ddlpackage_old-tdriver)

add_executable(dbcon_execplan_tdriver17 ../dbcon/execplan/tdriver17.cpp)
target_link_libraries(dbcon_execplan_tdriver17 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver17 COMMAND dbcon_execplan_tdriver17)

add_executable(dbcon_execplan_tdriver8 ../dbcon/execplan/tdriver8.cpp)
target_link_libraries(dbcon_execplan_tdriver8 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver8 COMMAND dbcon_execplan_tdriver8)

add_executable(dbcon_execplan_tdriver-sm ../dbcon/execplan/tdriver-sm.cpp)
target_link_libraries(dbcon_execplan_tdriver-sm ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver-sm COMMAND dbcon_execplan_tdriver-sm)

add_executable(dbcon_execplan_tdriver ../dbcon/execplan/tdriver.cpp)
target_link_libraries(dbcon_execplan_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver COMMAND dbcon_execplan_tdriver)

add_executable(dbcon_execplan_tdriver10 ../dbcon/execplan/tdriver10.cpp)
target_link_libraries(dbcon_execplan_tdriver10 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver10 COMMAND dbcon_execplan_tdriver10)

add_executable(dbcon_execplan_btdriver ../dbcon/execplan/btdriver.cpp)
target_link_libraries(dbcon_execplan_btdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_btdriver COMMAND dbcon_execplan_btdriver)

add_executable(dbcon_execplan_tdriver23 ../dbcon/execplan/tdriver23.cpp)
target_link_libraries(dbcon_execplan_tdriver23 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver23 COMMAND dbcon_execplan_tdriver23)

add_executable(dbcon_execplan_tdriver3 ../dbcon/execplan/tdriver3.cpp)
target_link_libraries(dbcon_execplan_tdriver3 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver3 COMMAND dbcon_execplan_tdriver3)

add_executable(dbcon_execplan_tdriver7 ../dbcon/execplan/tdriver7.cpp)
target_link_libraries(dbcon_execplan_tdriver7 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver7 COMMAND dbcon_execplan_tdriver7)

add_executable(dbcon_execplan_tdriver22 ../dbcon/execplan/tdriver22.cpp)
target_link_libraries(dbcon_execplan_tdriver22 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver22 COMMAND dbcon_execplan_tdriver22)

add_executable(dbcon_execplan_tdriver13 ../dbcon/execplan/tdriver13.cpp)
target_link_libraries(dbcon_execplan_tdriver13 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver13 COMMAND dbcon_execplan_tdriver13)

add_executable(dbcon_execplan_tdriver5 ../dbcon/execplan/tdriver5.cpp)
target_link_libraries(dbcon_execplan_tdriver5 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver5 COMMAND dbcon_execplan_tdriver5)

add_executable(dbcon_execplan_tdriver12 ../dbcon/execplan/tdriver12.cpp)
target_link_libraries(dbcon_execplan_tdriver12 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver12 COMMAND dbcon_execplan_tdriver12)

add_executable(dbcon_execplan_tdriver-oid ../dbcon/execplan/tdriver-oid.cpp)
target_link_libraries(dbcon_execplan_tdriver-oid ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver-oid COMMAND dbcon_execplan_tdriver-oid)

add_executable(dbcon_execplan_tdriver4 ../dbcon/execplan/tdriver4.cpp)
target_link_libraries(dbcon_execplan_tdriver4 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver4 COMMAND dbcon_execplan_tdriver4)

add_executable(dbcon_execplan_tdriver2 ../dbcon/execplan/tdriver2.cpp)
target_link_libraries(dbcon_execplan_tdriver2 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver2 COMMAND dbcon_execplan_tdriver2)

add_executable(dbcon_execplan_tdriver6 ../dbcon/execplan/tdriver6.cpp)
target_link_libraries(dbcon_execplan_tdriver6 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver6 COMMAND dbcon_execplan_tdriver6)

add_executable(dbcon_execplan_tdriver19 ../dbcon/execplan/tdriver19.cpp)
target_link_libraries(dbcon_execplan_tdriver19 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver19 COMMAND dbcon_execplan_tdriver19)

add_executable(dbcon_execplan_tdriver14 ../dbcon/execplan/tdriver14.cpp)
target_link_libraries(dbcon_execplan_tdriver14 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver14 COMMAND dbcon_execplan_tdriver14)

add_executable(dbcon_execplan_tdriver16 ../dbcon/execplan/tdriver16.cpp)
target_link_libraries(dbcon_execplan_tdriver16 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver16 COMMAND dbcon_execplan_tdriver16)

add_executable(dbcon_execplan_tdriver20 ../dbcon/execplan/tdriver20.cpp)
target_link_libraries(dbcon_execplan_tdriver20 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver20 COMMAND dbcon_execplan_tdriver20)

add_executable(dbcon_execplan_tdriver18 ../dbcon/execplan/tdriver18.cpp)
target_link_libraries(dbcon_execplan_tdriver18 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver18 COMMAND dbcon_execplan_tdriver18)

add_executable(dbcon_execplan_tdriver1 ../dbcon/execplan/tdriver1.cpp)
target_link_libraries(dbcon_execplan_tdriver1 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver1 COMMAND dbcon_execplan_tdriver1)

add_executable(dbcon_execplan_tdriver15 ../dbcon/execplan/tdriver15.cpp)
target_link_libraries(dbcon_execplan_tdriver15 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver15 COMMAND dbcon_execplan_tdriver15)

add_executable(dbcon_execplan_tdriver9 ../dbcon/execplan/tdriver9.cpp)
target_link_libraries(dbcon_execplan_tdriver9 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver9 COMMAND dbcon_execplan_tdriver9)

add_executable(dbcon_execplan_tdriver21 ../dbcon/execplan/tdriver21.cpp)
target_link_libraries(dbcon_execplan_tdriver21 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver21 COMMAND dbcon_execplan_tdriver21)

add_executable(dbcon_execplan_tdriver11 ../dbcon/execplan/tdriver11.cpp)
target_link_libraries(dbcon_execplan_tdriver11 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_execplan_tdriver11 COMMAND dbcon_execplan_tdriver11)

add_executable(dbcon_dmlpackageproc_tdriver ../dbcon/dmlpackageproc/tdriver.cpp)
target_link_libraries(dbcon_dmlpackageproc_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_dmlpackageproc_tdriver COMMAND dbcon_dmlpackageproc_tdriver)

add_executable(dbcon_joblist_tdriver-index ../dbcon/joblist/tdriver-index.cpp)
target_link_libraries(dbcon_joblist_tdriver-index ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_tdriver-index COMMAND dbcon_joblist_tdriver-index)

add_executable(dbcon_joblist_tdriver-hashjoin ../dbcon/joblist/tdriver-hashjoin.cpp)
target_link_libraries(dbcon_joblist_tdriver-hashjoin ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_tdriver-hashjoin COMMAND dbcon_joblist_tdriver-hashjoin)

add_executable(dbcon_joblist_tdriver-datalist ../dbcon/joblist/tdriver-datalist.cpp)
target_link_libraries(dbcon_joblist_tdriver-datalist ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_tdriver-datalist COMMAND dbcon_joblist_tdriver-datalist)

add_executable(dbcon_joblist_tdriver-jobstep ../dbcon/joblist/tdriver-jobstep.cpp)
target_link_libraries(dbcon_joblist_tdriver-jobstep ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_tdriver-jobstep COMMAND dbcon_joblist_tdriver-jobstep)

add_executable(dbcon_joblist_tdriver-bru ../dbcon/joblist/tdriver-bru.cpp)
target_link_libraries(dbcon_joblist_tdriver-bru ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_tdriver-bru COMMAND dbcon_joblist_tdriver-bru)

add_executable(dbcon_joblist_tdriver-function ../dbcon/joblist/tdriver-function.cpp)
target_link_libraries(dbcon_joblist_tdriver-function ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_tdriver-function COMMAND dbcon_joblist_tdriver-function)

add_executable(dbcon_joblist_tdriver-agg ../dbcon/joblist/tdriver-agg.cpp)
target_link_libraries(dbcon_joblist_tdriver-agg ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_tdriver-agg COMMAND dbcon_joblist_tdriver-agg)

add_executable(dbcon_joblist_tdriver-bdl ../dbcon/joblist/tdriver-bdl.cpp)
target_link_libraries(dbcon_joblist_tdriver-bdl ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_tdriver-bdl COMMAND dbcon_joblist_tdriver-bdl)

add_executable(dbcon_joblist_tdriver-zdl ../dbcon/joblist/tdriver-zdl.cpp)
target_link_libraries(dbcon_joblist_tdriver-zdl ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_tdriver-zdl COMMAND dbcon_joblist_tdriver-zdl)

add_executable(dbcon_joblist_orderby-tests ../dbcon/joblist/orderby-tests.cpp)
target_link_libraries(dbcon_joblist_orderby-tests ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_orderby-tests COMMAND dbcon_joblist_orderby-tests)

add_executable(dbcon_joblist_tdriver-pdict ../dbcon/joblist/tdriver-pdict.cpp)
target_link_libraries(dbcon_joblist_tdriver-pdict ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_tdriver-pdict COMMAND dbcon_joblist_tdriver-pdict)

add_executable(dbcon_joblist_tdriver-filter ../dbcon/joblist/tdriver-filter.cpp)
target_link_libraries(dbcon_joblist_tdriver-filter ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_joblist_tdriver-filter COMMAND dbcon_joblist_tdriver-filter)

add_executable(dbcon_dmlpackage_tdriver ../dbcon/dmlpackage/tdriver.cpp)
target_link_libraries(dbcon_dmlpackage_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:dbcon_dmlpackage_tdriver COMMAND dbcon_dmlpackage_tdriver)

add_executable(oamapps_replayTransactionLog_tdriver ../oamapps/replayTransactionLog/tdriver.cpp)
target_link_libraries(oamapps_replayTransactionLog_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:oamapps_replayTransactionLog_tdriver COMMAND oamapps_replayTransactionLog_tdriver)

add_executable(oamapps_sessionWalker_tdriver ../oamapps/sessionWalker/tdriver.cpp)
target_link_libraries(oamapps_sessionWalker_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:oamapps_sessionWalker_tdriver COMMAND oamapps_sessionWalker_tdriver)

add_executable(versioning_BRM_tdriver ../versioning/BRM/tdriver.cpp)
target_link_libraries(versioning_BRM_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:versioning_BRM_tdriver COMMAND versioning_BRM_tdriver)

add_executable(versioning_BRM_tdriver-dbrm2 ../versioning/BRM/tdriver-dbrm2.cpp)
target_link_libraries(versioning_BRM_tdriver-dbrm2 ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:versioning_BRM_tdriver-dbrm2 COMMAND versioning_BRM_tdriver-dbrm2)

add_executable(versioning_BRM_tdriver-brm ../versioning/BRM/tdriver-brm.cpp)
target_link_libraries(versioning_BRM_tdriver-brm ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:versioning_BRM_tdriver-brm COMMAND versioning_BRM_tdriver-brm)

add_executable(versioning_BRM_tdriver-dbrm ../versioning/BRM/tdriver-dbrm.cpp)
target_link_libraries(versioning_BRM_tdriver-dbrm ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:versioning_BRM_tdriver-dbrm COMMAND versioning_BRM_tdriver-dbrm)

add_executable(versioning_BRM_tdriver-load ../versioning/BRM/tdriver-load.cpp)
target_link_libraries(versioning_BRM_tdriver-load ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:versioning_BRM_tdriver-load COMMAND versioning_BRM_tdriver-load)

add_executable(writeengine_xml_txml ../writeengine/xml/txml.cpp)
target_link_libraries(writeengine_xml_txml ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:writeengine_xml_txml COMMAND writeengine_xml_txml)

add_executable(writeengine_index_qindex ../writeengine/index/qindex.cpp)
target_link_libraries(writeengine_index_qindex ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:writeengine_index_qindex COMMAND writeengine_index_qindex)

add_executable(writeengine_dictionary_tdctnry ../writeengine/dictionary/tdctnry.cpp)
target_link_libraries(writeengine_dictionary_tdctnry ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:writeengine_dictionary_tdctnry COMMAND writeengine_dictionary_tdctnry)

add_executable(writeengine_dictionary_qdctnry ../writeengine/dictionary/qdctnry.cpp)
target_link_libraries(writeengine_dictionary_qdctnry ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:writeengine_dictionary_qdctnry COMMAND writeengine_dictionary_qdctnry)

add_executable(writeengine_index_tindex ../writeengine/index/tindex.cpp)
target_link_libraries(writeengine_index_tindex ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:writeengine_index_tindex COMMAND writeengine_index_tindex)

add_executable(writeengine_wrapper_tdriver ../writeengine/wrapper/tdriver.cpp)
target_link_libraries(writeengine_wrapper_tdriver ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:writeengine_wrapper_tdriver COMMAND writeengine_wrapper_tdriver)

add_executable(primitives_linux-port_pp-scan-unittest ../primitives/linux-port/pp-scan-unittest.cpp)
target_link_libraries(primitives_linux-port_pp-scan-unittest ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
add_test(NAME columnstore:primitives_linux-port_pp-scan-unittest COMMAND primitives_linux-port_pp-scan-unittest)






# if (WITH_UNITTESTS)
#     cmake_policy(SET CMP0054 NEW)
#     set(EXTERNAL_INSTALL_LOCATION ${CMAKE_BINARY_DIR}/external)
#     ExternalProject_Add(googletest
#         GIT_REPOSITORY https://github.com/google/googletest
#         GIT_TAG release-1.12.0
#         CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION} -DBUILD_SHARED_LIBS=ON
#     )

#     # this is a workaround for gtest's bug still not closed
#     # https://github.com/google/googletest/issues/3659
#     if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
#         MY_CHECK_AND_SET_COMPILER_FLAG("-U__cpp_char8_t")
#     endif()


#     include_directories(${EXTERNAL_INSTALL_LOCATION}/include)
#     # lib64 for RPM-based distros
#     link_directories(${EXTERNAL_INSTALL_LOCATION}/lib ${EXTERNAL_INSTALL_LOCATION}/lib64)
#     set(GTEST_LIBRARIES gtest gtest_main pthread)
#     include(GoogleTest)

#     #GoogleTest tests
#     add_executable(rowgroup_tests rowgroup-tests.cpp)
#     add_dependencies(rowgroup_tests googletest)
#     target_link_libraries(rowgroup_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS})
#     gtest_add_tests(TARGET rowgroup_tests TEST_PREFIX columnstore:)

#     add_executable(mcs_decimal_tests mcs_decimal-tests.cpp)
#     add_dependencies(mcs_decimal_tests googletest)
#     target_link_libraries(mcs_decimal_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS})
#     gtest_add_tests(TARGET mcs_decimal_tests TEST_PREFIX columnstore:)

#     add_executable(dataconvert_tests dataconvert-tests.cpp)
#     add_dependencies(dataconvert_tests googletest)
#     target_link_libraries(dataconvert_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS})
#     gtest_add_tests(TARGET dataconvert_tests TEST_PREFIX columnstore:)

#     add_executable(rebuild_em_tests rebuild-em-tests.cpp)
#     add_dependencies(rebuild_em_tests googletest)
#     target_link_libraries(rebuild_em_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_WRITE_LIBS})
#     gtest_add_tests(TARGET rebuild_em_tests TEST_PREFIX columnstore:)

#     add_executable(compression_tests compression-tests.cpp)
#     add_dependencies(compression_tests googletest)
#     target_link_libraries(compression_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_WRITE_LIBS})
#     gtest_add_tests(TARGET compression_tests TEST_PREFIX columnstore:)

#     add_executable(column_scan_filter_tests primitives_column_scan_and_filter.cpp)
#     target_compile_options(column_scan_filter_tests PRIVATE -Wno-error -Wno-sign-compare)
#     add_dependencies(column_scan_filter_tests googletest)
#     target_link_libraries(column_scan_filter_tests ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${GTEST_LIBRARIES} processor dbbc)
#     gtest_add_tests(TARGET column_scan_filter_tests TEST_PREFIX columnstore:)

#     add_executable(simd_processors simd_processors.cpp)
#     target_compile_options(simd_processors PRIVATE -Wno-error)
#     add_dependencies(simd_processors googletest)
#     target_link_libraries(simd_processors ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${GTEST_LIBRARIES} processor dbbc)
#     gtest_add_tests(TARGET simd_processors TEST_PREFIX columnstore:)

#     add_executable(fair_threadpool_test fair_threadpool.cpp)
#     add_dependencies(fair_threadpool_test googletest)
#     target_link_libraries(fair_threadpool_test ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${GTEST_LIBRARIES} processor dbbc)
#     gtest_add_tests(TARGET fair_threadpool_test TEST_PREFIX columnstore:)

#     add_executable(comparators_tests comparators-tests.cpp)
#     target_link_libraries(comparators_tests ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
#     add_test(NAME columnstore:comparators_tests COMMAND comparators_tests)

#     add_executable(patterns_test patterns_test.cpp)
#     add_dependencies(patterns_test googletest mpark_patterns)
#     target_include_directories(patterns_test PUBLIC ${ENGINE_COMMON_INCLUDES} ${ENGINE_BLOCKCACHE_INCLUDE} ${ENGINE_PRIMPROC_INCLUDE} )
#     target_link_libraries(patterns_test ${GTEST_LIBRARIES})
#     gtest_add_tests(TARGET patterns_test TEST_PREFIX mpark_patterns:)

#     # standalone EM routines test
#     add_executable(brm_em_standalone brm-em-standalone.cpp)
#     target_link_libraries(brm_em_standalone ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
#     add_test(NAME columnstore:brm_em_standalone COMMAND brm_em_standalone)
#     set_tests_properties(columnstore:brm_em_standalone PROPERTIES DISABLED True)
# endif()

# if (WITH_MICROBENCHMARKS AND (NOT CMAKE_BUILD_TYPE STREQUAL "Debug"))
#     find_package(benchmark REQUIRED)
#     add_executable(primitives_scan_bench primitives_scan_bench.cpp)
#     target_include_directories(primitives_scan_bench PUBLIC ${ENGINE_COMMON_INCLUDES} ${ENGINE_BLOCKCACHE_INCLUDE} ${ENGINE_PRIMPROC_INCLUDE} )
#     target_link_libraries(primitives_scan_bench ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${GTEST_LIBRARIES} processor dbbc benchmark::benchmark)
#     add_test(NAME columnstore_microbenchmarks:primitives_scan_bench, COMMAND primitives_scan_bench)
# endif()

