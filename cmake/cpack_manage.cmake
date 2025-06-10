macro(columnstore_append_for_cpack var_name)
    # Get current value from parent scope or use empty string
    if(DEFINED ${var_name})
        set(current_val "${${var_name}}")
    else()
        set(current_val "")
    endif()

    # Process each argument to append
    foreach(arg IN LISTS ARGN)
        if(current_val)
            # If not empty, add comma before new item
            set(current_val "${current_val}, ${arg}")
        else()
            # If empty, just add the item
            set(current_val "${arg}")
        endif()
    endforeach()

    # Set back in parent scope
    set(${var_name}
        "${current_val}"
        PARENT_SCOPE
    )
endmacro()

macro(columnstore_add_rpm_deps)
    columnstore_append_for_cpack(CPACK_RPM_columnstore-engine_PACKAGE_REQUIRES ${ARGN})
endmacro()

if(RPM)
    columnstore_add_rpm_deps("snappy" "jemalloc" "procps-ng" "gawk")
endif()
