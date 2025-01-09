set(EXTERNAL_INCLUDE_DIR "${CMAKE_BINARY_DIR}/external/include")
file(MAKE_DIRECTORY "${EXTERNAL_INCLUDE_DIR}")

set(C11CLI_URL "https://github.com/CLIUtils/CLI11/releases/download/v2.4.2/CLI11.hpp")
set(C11CLI_HEADER "${EXTERNAL_INCLUDE_DIR}/CLI11.hpp")
set(CLI11_INCLUDE_DIR "${EXTERNAL_INCLUDE_DIR}")

file(DOWNLOAD 
    ${C11CLI_URL}
    ${C11CLI_HEADER}
    SHOW_PROGRESS STATUS download_status
)

add_library(CLI11 INTERFACE)
target_include_directories(CLI11 INTERFACE ${CLI11_INCLUDE_DIR})