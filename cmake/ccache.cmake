
OPTION(USE_CCACHE "reduce compile time with ccache." FALSE)
find_program(CCACHE NAMES ccache ccache.exe)

if(CCACHE AND USE_CCACHE)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif()
