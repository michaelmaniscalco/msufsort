
find_library(LIBCXX_LIB c++)
find_library(LIBCXXABI_LIB c++abi)

add_compile_options(
    -stdlib=libc++
    -O3
    -march=native
)

link_libraries(
    ${LIBCXX_LIB}
    ${LIBCXXABI_LIB}
)

add_executable(msufsort_demo main.cpp)

target_link_libraries(msufsort_demo ${CMAKE_THREAD_LIBS_INIT} msufsort)