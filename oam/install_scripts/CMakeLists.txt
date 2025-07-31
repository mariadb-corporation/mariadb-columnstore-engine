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
    set(ABORT_ON_ERROR "1")

    if(COLUMNSTORE_PACKAGES_BUILD)
        set(ABORT_ON_ERROR "0")
    endif()

    set(ASAN_OPTIONS
        abort_on_error=${ABORT_ON_ERROR}:disable_coredump=0:print_stats=0:detect_odr_violation=0:check_initialization_order=1:detect_stack_use_after_return=1:atexit=0
    )
    set(ALLOC_CONFIG ASAN_OPTIONS=${ASAN_OPTIONS})

    set(PRIMPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.primproc)
    set(DMLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.dmlproc)
    set(DDLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.ddlproc)
    set(WRITEENGINE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.writeengine)
    set(CONTROLLERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.controllernode)
    set(WORKERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.workernode)
    set(STORAGEMANAGER_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${ASAN_PATH}.storagemanager)
    set(COLUMNSTORE_STANDALONE_BINARIES_ASAN_COPTIONS ${ASAN_OPTIONS})
    configure_file(
        "${CMAKE_SOURCE_DIR}/storage/columnstore/columnstore/build/asan_options.cpp.in"
        "${CMAKE_BINARY_DIR}/asan_options.cpp" @ONLY
    )

endif()

if(WITH_UBSAN)
    set(UBSAN_PATH "/tmp/asan")
    if(WITH_COLUMNSTORE_REPORT_PATH)
        set(UBSAN_PATH "${WITH_COLUMNSTORE_REPORT_PATH}/ubsan")
    endif(WITH_COLUMNSTORE_REPORT_PATH)

    set(LD_PRELOAD_STRING "")
    set(UBSAN_OPTIONS abort_on_error=0:print_stacktrace=1)
    set(ALLOC_CONFIG UBSAN_OPTIONS=${UBSAN_OPTIONS})
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
    set(TSAN_OPTIONS abort_on_error=0:log_path=${TSAN_PATH})
    set(ALLOC_CONFIG TSAN_OPTIONS=${TSAN_OPTIONS})
    set(PRIMPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.primproc)
    set(DMLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.dmlproc)
    set(DDLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.ddlproc)
    set(WRITEENGINE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.writeengine)
    set(CONTROLLERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.controllernode)
    set(WORKERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.workernode)
    set(STORAGEMANAGER_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${TSAN_PATH}.storagemanager)
endif()

if(WITH_MSAN)
    set(MSAN_PATH "/tmp/msan")
    if(WITH_COLUMNSTORE_REPORT_PATH)
        set(MSAN_PATH "${WITH_COLUMNSTORE_REPORT_PATH}/msan")
    endif(WITH_COLUMNSTORE_REPORT_PATH)

    set(LD_PRELOAD_STRING "")
    set(MSAN_OPTIONS abort_on_error=0:log_path=${MSAN_PATH})
    set(ALLOC_CONFIG MSAN_OPTIONS=${MSAN_OPTIONS})
    set(PRIMPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${MSAN_PATH}.primproc)
    set(DMLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${MSAN_PATH}.dmlproc)
    set(DDLPROC_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${MSAN_PATH}.ddlproc)
    set(WRITEENGINE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${MSAN_PATH}.writeengine)
    set(CONTROLLERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${MSAN_PATH}.controllernode)
    set(WORKERNODE_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${MSAN_PATH}.workernode)
    set(STORAGEMANAGER_ALLOC_CONFIG ${ALLOC_CONFIG},log_path=${MSAN_PATH}.storagemanager)
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

columnstore_install_program(columnstore-post-install ${ENGINE_BINDIR})
columnstore_install_program(columnstore-pre-uninstall ${ENGINE_BINDIR})
columnstore_install_program(columnstore_run.sh ${ENGINE_BINDIR})
columnstore_install_program(post-mysql-install ${ENGINE_BINDIR})
columnstore_install_program(post-mysqld-install ${ENGINE_BINDIR})
columnstore_install_program(columnstoreSyslogSetup.sh ${ENGINE_BINDIR})
columnstore_install_program(mcs-stop-controllernode.sh ${ENGINE_BINDIR})
columnstore_install_program(mcs-loadbrm.py ${ENGINE_BINDIR})
columnstore_install_program(mcs-savebrm.py ${ENGINE_BINDIR})
columnstore_install_program(mariadb-columnstore-start.sh ${ENGINE_BINDIR})
columnstore_install_program(mariadb-columnstore-stop.sh ${ENGINE_BINDIR})
columnstore_install_program(loop_process_starter.sh ${ENGINE_BINDIR})

columnstore_install_file(mariadb-columnstore.service ${ENGINE_SUPPORTDIR})
columnstore_install_file(columnstoreAlias ${ENGINE_SUPPORTDIR})
columnstore_install_file(columnstoreSyslog ${ENGINE_SUPPORTDIR})
columnstore_install_file(columnstoreSyslog7 ${ENGINE_SUPPORTDIR})
columnstore_install_file(columnstoreSyslog-ng ${ENGINE_SUPPORTDIR})
columnstore_install_file(columnstoreLogRotate ${ENGINE_SUPPORTDIR})
columnstore_install_file(myCnf-include-args.text ${ENGINE_SUPPORTDIR})
columnstore_install_file(myCnf-exclude-args.text ${ENGINE_SUPPORTDIR})
columnstore_install_file(mcs-workernode.service ${ENGINE_SUPPORTDIR})
columnstore_install_file(mcs-controllernode.service ${ENGINE_SUPPORTDIR})
columnstore_install_file(mcs-primproc.service ${ENGINE_SUPPORTDIR})
columnstore_install_file(mcs-writeengineserver.service ${ENGINE_SUPPORTDIR})
columnstore_install_file(mcs-dmlproc.service ${ENGINE_SUPPORTDIR})
columnstore_install_file(mcs-ddlproc.service ${ENGINE_SUPPORTDIR})
columnstore_install_file(mcs-loadbrm.service ${ENGINE_SUPPORTDIR})
columnstore_install_file(mcs-storagemanager.service ${ENGINE_SUPPORTDIR})

columnstore_install_file(module ${ENGINE_DATADIR}/local)

find_package(Python3 COMPONENTS Interpreter)
if(Python3_FOUND)
    add_test(NAME PythonUnitTests COMMAND ${Python3_EXECUTABLE} -m unittest test_mcs-savebrm.py)
endif()
