
# autoInstaller 

cs_add_executable(autoInstaller autoInstaller.cpp)

target_link_libraries(autoInstaller 
  PRIVATE 
    brm
    Readline::Readline
  )

install(TARGETS autoInstaller DESTINATION ${CMAKE_INSTALL_BINDIR})



# autoConfigure

cs_add_executable(autoConfigure autoConfigure.cpp)

target_link_libraries(autoConfigure 
  PRIVATE 
    brm
  )
install(TARGETS autoConfigure DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)
                                                                                                                 