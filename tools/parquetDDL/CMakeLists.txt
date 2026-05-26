include_directories(${ENGINE_COMMON_INCLUDES})

if(NOT PARQUET_ENABLED OR NOT ARROW_ENABLED)
    message(STATUS "Skipping mcs_parquet_ddl: Arrow/Parquet not enabled")
    return()
endif()

set(parquetDDL_SRCS main.cpp)

add_executable(mcs_parquet_ddl ${parquetDDL_SRCS})

# Arrow / Parquet public headers emit -Wunused-parameter and (with GCC)
# -Wmissing-braces when parent targets use strict warnings and -Werror.
if(NOT MSVC)
    set_source_files_properties(
        "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp"
        PROPERTIES COMPILE_OPTIONS "-Wno-unused-parameter;-Wno-missing-braces"
    )
endif()

target_compile_definitions(mcs_parquet_ddl PRIVATE WITH_ARROW=1)
target_include_directories(
    mcs_parquet_ddl PRIVATE ${PARQUET_INCLUDE_DIRS} ${ARROW_INCLUDE_DIRS}
)
columnstore_link(mcs_parquet_ddl ${PARQUET_LIBRARIES} ${ARROW_LIBRARIES})

if(PARQUET_EXTERNAL_TARGET)
    add_dependencies(mcs_parquet_ddl ${PARQUET_EXTERNAL_TARGET})
endif()
if(ARROW_EXTERNAL_TARGET)
    add_dependencies(mcs_parquet_ddl ${ARROW_EXTERNAL_TARGET})
endif()

install(TARGETS mcs_parquet_ddl DESTINATION ${ENGINE_BINDIR}
        COMPONENT columnstore-engine)
