add_library(msufsort
    msufsort/msufsort.cpp
)

target_include_directories(msufsort
    PUBLIC
        $<BUILD_INTERFACE:${_msufsort_include_dir}>
        $<INSTALL_INTERFACE:include/msufsort>
)

target_compile_features(msufsort PUBLIC cxx_std_17)