set(DEFAULT_USER "mysql")
set(DEFAULT_GROUP "mysql")
set(CORE_DUMPS "0")
option(WITH_COREDUMPS "dump cores" OFF)

if(WITH_COREDUMPS)
    set(CORE_DUMPS "infinity")
endif(WITH_COREDUMPS)

set(LD_PRELOAD_STRING "LD_PRELOAD=$(ldconfig -p | grep -m1 libjemalloc | awk '{print $1}')")
set(ALLOC_CONFIG "MALLOC_CONF=''")
set(PRIMPROC_ALLOC_CONFIG ${ALLOC_CONFIG})
set(DMLPROC_ALLOC_CONFIG ${ALLOC_CONFIG})
set(DDLPROC_ALLOC_CONFIG ${ALLOC_CONFIG})
set(WRITEENGINE_ALLOC_CONFIG ${ALLOC_CONFIG})
set(CONTROLLERNODE_ALLOC_CONFIG ${ALLOC_CONFIG})
set(WORKERNODE_ALLOC_CONFIG ${ALLOC_CONFIG})
set(STORAGEMANAGER_ALLOC_CONFIG ${ALLOC_CONFIG})

if(WITH_COLUMNSTORE_ASAN)
    set(ASAN_PATH "/tmp/asan")
    if(WITH_COLUMNSTORE_REPORT_PATH)
        set(ASAN_PATH "${WITH_COLUMNSTORE_REPORT_PATH}/asan")
    endif(WITH_COLUMNSTORE_REPORT_PATH)

    set(LD_PRELOAD_STRING "")
    set(ALLOC_CONFIG
        "ASAN_OPTIONS=abort_on_error=1:disable_coredump=0,print_stats=false,detect_odr_violation=0,check_initialization_order=1,detect_stack_use_after_return=1,atexit=false"
    )
    set(PRIMPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.primproc)
    set(DMLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.dmlproc)
    set(DDLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.ddlproc)
    set(WRITEENGINE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.writeengine)
    set(CONTROLLERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.controllernode)
    set(WORKERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.workernode)
    set(STORAGEMANAGER_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.storagemanager)
endif()

if(WITH_UBSAN)
    set(UBSAN_PATH "/tmp/asan")
    if(WITH_COLUMNSTORE_REPORT_PATH)
        set(UBSAN_PATH "${WITH_COLUMNSTORE_REPORT_PATH}/ubsan")
    endif(WITH_COLUMNSTORE_REPORT_PATH)

    set(LD_PRELOAD_STRING "")
    set(ALLOC_CONFIG "UBSAN_OPTIONS=abort_on_error=0,print_stacktrace=true")
    set(PRIMPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${UBSAN_PATH}.primproc)
    set(DMLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${UBSAN_PATH}.dmlproc)
    set(DDLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${UBSAN_PATH}.ddlproc)
    set(WRITEENGINE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${UBSAN_PATH}.writeengine)
    set(CONTROLLERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${UBSAN_PATH}.controllernode)
    set(WORKERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${UBSAN_PATH}.workernode)
    set(STORAGEMANAGER_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${UBSAN_PATH}.storagemanager)
endif()

if(WITH_TSAN)
    set(TSAN_PATH "/tmp/tsan")
    if(WITH_COLUMNSTORE_REPORT_PATH)
        set(TSAN_PATH "${WITH_COLUMNSTORE_REPORT_PATH}/tsan")
    endif(WITH_COLUMNSTORE_REPORT_PATH)

    set(LD_PRELOAD_STRING "")
    set(ALLOC_CONFIG "TSAN_OPTIONS=abort_on_error=0:log_path=${TSAN_PATH}")
    set(PRIMPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.primproc)
    set(DMLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.dmlproc)
    set(DDLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.ddlproc)
    set(WRITEENGINE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.writeengine)
    set(CONTROLLERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.controllernode)
    set(WORKERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.workernode)
    set(STORAGEMANAGER_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.storagemanager)
endif()

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/columnstoreSyslogSetup.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstoreSyslogSetup.sh"
    @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/columnstore-post-install.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstore-post-install"
    @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/columnstore-pre-uninstall.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstore-pre-uninstall"
    @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/columnstoreLogRotate.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstoreLogRotate" @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/mariadb-columnstore.service.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/mariadb-columnstore.service" @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/mariadb-columnstore-start.sh.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/mariadb-columnstore-start.sh" @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-workernode.service.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcs-workernode.service" @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-controllernode.service.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-controllernode.service" @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-primproc.service.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcs-primproc.service" @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-writeengineserver.service.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-writeengineserver.service" @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-dmlproc.service.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcs-dmlproc.service" @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-ddlproc.service.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcs-ddlproc.service" @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-loadbrm.service.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcs-loadbrm.service" @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-storagemanager.service.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-storagemanager.service" @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-stop-controllernode.sh.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/mcs-stop-controllernode.sh" @ONLY
)

set(SHMEM_FILE_GLOB "MCS-shm-")
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/mcs-loadbrm.py.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcs-loadbrm.py" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/mcs-savebrm.py.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcs-savebrm.py" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/mcs-savebrm.py.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcssavebrm.py" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/columnstoreSyslog.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstoreSyslog" @ONLY)

install(
    PROGRAMS columnstore-post-install
             columnstore-pre-uninstall
             columnstore_run.sh
             post-mysql-install
             post-mysqld-install
             columnstoreSyslogSetup.sh
             mcs-stop-controllernode.sh
             mcs-loadbrm.py
             mcs-savebrm.py
             mariadb-columnstore-start.sh
             mariadb-columnstore-stop.sh
             loop_process_starter.sh
    DESTINATION ${ENGINE_BINDIR}
    COMPONENT columnstore-engine
)

install(
    FILES mariadb-columnstore.service
          columnstoreAlias
          columnstoreSyslog
          columnstoreSyslog7
          columnstoreSyslog-ng
          columnstoreLogRotate
          myCnf-include-args.text
          myCnf-exclude-args.text
          mcs-workernode.service
          mcs-controllernode.service
          mcs-primproc.service
          mcs-writeengineserver.service
          mcs-dmlproc.service
          mcs-ddlproc.service
          mcs-loadbrm.service
          mcs-storagemanager.service
    DESTINATION ${ENGINE_SUPPORTDIR}
    COMPONENT columnstore-engine
)

install(
    FILES module
    DESTINATION ${ENGINE_DATADIR}/local
    COMPONENT columnstore-engine
)

find_package(Python3 COMPONENTS Interpreter)
if(Python3_FOUND)
    add_test(NAME PythonUnitTests COMMAND ${Python3_EXECUTABLE} -m unittest test_mcs-savebrm.py)
endif()
