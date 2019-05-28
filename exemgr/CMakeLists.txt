
add_executable(ExeMgr main.cpp activestatementcounter.cpp femsghandler.cpp ../utils/common/crashtrace.cpp)

target_link_libraries(ExeMgr 
  PRIVATE Boost::filesystem
          Boost::system
          Boost::thread
          mariadb
          alarmmanager_h
          brm_h
          common_h
          configcpp_h
          dataconvert_h
          execplan_h
          funcexp_h
          idbboot_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h
          querystats_h
          querytele_h
          rowgroup_h
          threadpool_h
          udfsdk_h)

target_pch(ExeMgr)


########### install files ###############

install(TARGETS ExeMgr DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
