# msufsort
msufsort suffix array construction algorithm

high performance, multi-threaded, suffix array, bwt/unbwt, lcp construction algorithm

**** this is a pre-release demo ****
**** this version is incomplete and lacks induction sorting which can result in sub optimal performance on some pathological inputs ****


======================================================================

To compile:

```
mkdir build
cd build
cmake ..
make
```

To build demo:

```
mkdir build
cd build
cmake -DMSUFSORT_BUILD_DEMO=ON ..
make 
```
