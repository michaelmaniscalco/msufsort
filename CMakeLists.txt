cmake_minimum_required(VERSION 3.5.1)

set(CMAKE_INSTALL_PREFIX /usr/local)
set(PROJECT_NAME msufsort)
project(${PROJECT_NAME})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_BUILD_TYPE Release)

add_compile_options(
    -O3
    -march=native
    -pipe
)

find_package(Threads)

include_directories(./src)

file(GLOB SOURCES ./src/library/msufsort/*.cpp)

add_library(msufsort STATIC ${SOURCES})
set_target_properties(msufsort PROPERTIES PUBLIC_HEADER ./src/library/msufsort/msufsort.h)

add_executable(msufsort_demo ./src/executable/msufsort/main.cpp)

target_link_libraries(msufsort_demo ${CMAKE_THREAD_LIBS_INIT} msufsort msufsort)
set_target_properties(msufsort_demo PROPERTIES OUTPUT_NAME msufsort)

install(TARGETS msufsort msufsort_demo
    EXPORT msufsort-targets
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/
    ARCHIVE DESTINATION lib
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_PREFIX}/include/maniscalco/
    )

export(EXPORT ${PROJECT_NAME}-targets FILE ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-targets.cmake)
export(PACKAGE ${PROJECT_NAME})
