find_library(LIBCXX_LIB c++)
find_package(Threads)
find_library(LIBCXXABI_LIB c++abi)

link_libraries(
    ${LIBCXX_LIB}
    ${LIBCXXABI_LIB}
)

add_library(msufsort
    msufsort.cpp
)

target_link_libraries(msufsort ${CMAKE_THREAD_LIBS_INIT})

target_include_directories(msufsort
    PUBLIC
        $<BUILD_INTERFACE:${_msufsort_include_dir}>
        $<INSTALL_INTERFACE:include/msufsort>
)

target_compile_features(msufsort PUBLIC cxx_std_17)
