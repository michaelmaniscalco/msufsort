cmake_minimum_required(VERSION 3.8)

project(msufsort
    LANGUAGES CXX
)

option(MSUFSORT_BUILD_DEMO "Build the CLI demo" OFF)

if (MSUFSORT_BUILD_DEMO)
    set(CMAKE_BUILD_TYPE Release)
    set(CMAKE_CXX_COMPILER "clang++") 
    find_library(LIBCXX_LIB c++)
    find_package(Threads)
    add_compile_options(
        -stdlib=libc++
        -O3
        -g
        -march=native
    )
    link_libraries(
        ${LIBCXX_LIB}
    )
endif()

add_subdirectory(src)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

