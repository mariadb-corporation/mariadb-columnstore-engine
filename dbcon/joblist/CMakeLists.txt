INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES} ${ENGINE_SRC_DIR}/tools/passwd)
# ########## next target ###############

SET (
  joblist_LIB_SRCS
  anydatalist.cpp
  batchprimitiveprocessor-jl.cpp
  columncommand-jl.cpp
  command-jl.cpp
  crossenginestep.cpp
  dictstep-jl.cpp
  diskjoinstep.cpp
  distributedenginecomm.cpp
  elementtype.cpp
  expressionstep.cpp
  filtercommand-jl.cpp
  filterstep.cpp
  groupconcat.cpp
  jl_logger.cpp
  jlf_common.cpp
  jlf_execplantojoblist.cpp
  jlf_graphics.cpp
  jlf_tuplejoblist.cpp
  jlf_subquery.cpp
  joblist.cpp
  joblistfactory.cpp
  jobstep.cpp
  jobstepassociation.cpp
  jsonarrayagg.cpp
  lbidlist.cpp
  limitedorderby.cpp
  passthrucommand-jl.cpp
  passthrustep.cpp
  pcolscan.cpp
  pcolstep.cpp
  pdictionary.cpp
  pdictionaryscan.cpp
  pseudocc-jl.cpp
  resourcedistributor.cpp
  resourcemanager.cpp
  rowestimator.cpp
  rtscommand-jl.cpp
  subquerystep.cpp
  subquerytransformer.cpp
  tablecolumn.cpp
  timestamp.cpp
  tuple-bps.cpp
  tupleaggregatestep.cpp
  tupleannexstep.cpp
  tupleconstantstep.cpp
  tuplehashjoin.cpp
  tuplehavingstep.cpp
  tupleunion.cpp
  unique32generator.cpp
  virtualtable.cpp
  windowfunctionstep.cpp
  ${ENGINE_SRC_DIR}/tools/passwd/secrets.cpp
)

# ########## next target ###############

ADD_LIBRARY (joblist_server STATIC ${joblist_LIB_SRCS})
ADD_DEPENDENCIES (joblist_server loggingcpp)

# ########## next target ###############

ADD_DEFINITIONS (-DMYSQL_SERVICE_THD_TIMEZONE_INCLUDED)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (joblist STATIC ${joblist_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (joblist SHARED ${joblist_LIB_SRCS})
ENDIF ()

TARGET_INCLUDE_DIRECTORIES (
  joblist BEFORE PUBLIC ${OPENSSL_INCLUDE_DIR} ${LIBMARIADB_BININC_DIR} ${LIBMARIADB_SRCINC_DIR}
)
ADD_DEPENDENCIES (joblist loggingcpp)

INSTALL (
  TARGETS joblist
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)

IF (WITH_ORDERBY_UT)
  ADD_EXECUTABLE (job_orderby_tests orderby-tests.cpp)
  TARGET_LINK_LIBRARIES (job_orderby_tests ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
  INSTALL (
    TARGETS job_orderby_tests
    DESTINATION ${ENGINE_BINDIR}
    COMPONENT columnstore-engine
  )
ENDIF ()
