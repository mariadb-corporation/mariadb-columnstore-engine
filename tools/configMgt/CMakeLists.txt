
# autoInstaller 

add_executable(autoInstaller autoInstaller.cpp)

target_link_libraries(autoInstaller 
  PRIVATE 
    brm
    Readline::Readline
  )

install(TARGETS autoInstaller DESTINATION ${ENGINE_BINDIR})



# autoConfigure

add_executable(autoConfigure autoConfigure.cpp)

target_link_libraries(autoConfigure 
  PRIVATE 
    brm
  )
install(TARGETS autoConfigure DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
                                                                                                                 