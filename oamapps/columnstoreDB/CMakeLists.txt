
add_executable(columnstoreDBWrite columnstoreDB.cpp)

target_link_libraries(columnstoreDBWrite 
  PRIVATE 
    brm
    common_h
    idbboot_h
    joblist_h
    )

install(TARGETS columnstoreDBWrite DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

