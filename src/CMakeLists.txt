set(_msufsort_include_dir ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(library)

if (MSUFSORT_BUILD_DEMO)
    add_subdirectory(executable)
endif()