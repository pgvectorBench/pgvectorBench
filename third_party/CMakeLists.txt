set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

add_subdirectory(argparse)
add_subdirectory(concurrentqueue)
add_subdirectory(ryu)
add_subdirectory(spdlog)

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    target_compile_definitions(spdlog 
        INTERFACE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
    target_compile_definitions(spdlog_header_only
        INTERFACE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
endif()
