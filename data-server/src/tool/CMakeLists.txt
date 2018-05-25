
set (TOOLSOURCE
    check_range.cpp
)

foreach(toolfile ${TOOLSOURCE})
    get_filename_component(exename ${toolfile} NAME_WE)
    add_executable(${exename} ${toolfile})
endforeach(toolfile ${TOOLSOURCE})
