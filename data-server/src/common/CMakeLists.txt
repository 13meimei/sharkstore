set(common_SOURCES
    ds_config.c
        ds_encoding.cpp
    rpc_request.cpp
)

foreach(f IN LISTS common_SOURCES) 
    set_source_files_properties(${f} PROPERTIES 
        COMPILE_DEFINITIONS "__FNAME__=\"common/${f}\"") 
endforeach() 

add_library(sharkstore-common STATIC ${common_SOURCES})
