#pragma once

#include <cstdint>
#include <type_traits>

#ifdef __APPLE__
    #include <libkern/OSByteOrder.h>
#else
    #include <byteswap.h>
#endif


namespace maniscalco
{

    //==============================================================================
    template <typename T>
    auto byte_swap
    (
        T value
    ) -> typename std::enable_if<sizeof(T) == sizeof(std::uint8_t), T>::type
    {
        return value;
    }


    //==============================================================================
    template <typename T>
    auto byte_swap
    (
        T value
    ) -> typename std::enable_if<sizeof(T) == sizeof(std::uint16_t), T>::type
    {
        auto v = static_cast<std::uint16_t>(value);
        return static_cast<T>((v >> 8) | (v << 8));
    }


    //==============================================================================
    template <typename T>
    auto byte_swap
    (
        T value
    ) -> typename std::enable_if<sizeof(T) == sizeof(std::uint32_t), T>::type
    {
        #ifdef __APPLE__
            return static_cast<T>(OSSwapInt32(static_cast<uint32_t>(value)));
        #else
            return static_cast<T>(__builtin_bswap32(static_cast<uint32_t>(value)));
        #endif
    }


    //==============================================================================
    template <typename T>
    auto byte_swap
    (
        T value
    ) -> typename std::enable_if<sizeof(T) == sizeof(std::uint64_t), T>::type
    {
        #ifdef __APPLE__
            return static_cast<T>(OSSwapInt64(static_cast<uint64_t>(value)));
        #else
            return static_cast<T>(__builtin_bswap64(static_cast<uint64_t>(value)));
        #endif
    }

} // namespace maniscalco
