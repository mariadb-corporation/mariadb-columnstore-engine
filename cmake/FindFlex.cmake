

FIND_PROGRAM(LEX_EXECUTABLE flex DOC "path to the flex executable")
if(NOT LEX_EXECUTABLE)
    FIND_PROGRAM(LEX_EXECUTABLE lex DOC "path to the lex executable")
endif()

include(FindPackageHandleStandardArgs)

FIND_PACKAGE_HANDLE_STANDARD_ARGS(Flex REQUIRED_VARS LEX_EXECUTABLE)

mark_as_advanced(LEX_EXECUTABLE)