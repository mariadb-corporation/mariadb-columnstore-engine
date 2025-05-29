include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(dmlpackageproc_LIB_SRCS
    deletepackageprocessor.cpp
    dmlpackageprocessor.cpp
    insertpackageprocessor.cpp
    updatepackageprocessor.cpp
    commandpackageprocessor.cpp
    autoincrementdata.cpp
    tablelockdata.cpp
)

columnstore_library(dmlpackageproc ${dmlpackageproc_LIB_SRCS})
columnstore_link(dmlpackageproc loggingcpp oamcpp messageqcpp)
