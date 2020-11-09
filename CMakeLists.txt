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

include(GNUInstallDirs)

install(
    TARGETS msufsort
    EXPORT msufsort-export
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
	RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(EXPORT msufsort-export
    FILE
        msufsortTargets.cmake
    NAMESPACE
        msufsort::
    DESTINATION
        ${CMAKE_INSTALL_LIBDIR}/cmake/msufsort
)

set(_msufsort_config "${CMAKE_CURRENT_BINARY_DIR}/msufsort-config.cmake")
set(MSUFSORT_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/")

file(WRITE
    ${_msufsort_config}
    "include(\"\${CMAKE_CURRENT_LIST_DIR}/msufsortTargets.cmake\")"
)

install(
    FILES ${_msufsort_config}
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/msufsort
)

install(
    DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}/src/library/msufsort
    DESTINATION include/msufsort/
    FILES_MATCHING PATTERN "*.h"
)
