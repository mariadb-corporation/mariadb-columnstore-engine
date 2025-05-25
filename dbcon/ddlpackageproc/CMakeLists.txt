include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(ddlpackageproc_LIB_SRCS
    ddlpackageprocessor.cpp
    createtableprocessor.cpp
    altertableprocessor.cpp
    droptableprocessor.cpp
    markpartitionprocessor.cpp
    restorepartitionprocessor.cpp
    droppartitionprocessor.cpp
)

columnstore_library(ddlpackageproc ${ddlpackageproc_LIB_SRCS})
columnstore_link(ddlpackageproc ${NETSNMP_LIBRARIES} loggingcpp)
