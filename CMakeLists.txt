include_directories(storage-manager/include)

set(storagemanager_SRCS main.cpp SessionManager.cpp)

add_executable(StorageManager ${storagemanager_SRCS})

//install(TARGETS StorageManager DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


