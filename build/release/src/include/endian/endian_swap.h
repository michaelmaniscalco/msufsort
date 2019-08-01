#pragma once

#include "./endian_type.h"
#include "./byte_swap.h"
#include <type_traits>


namespace maniscalco
{

    namespace impl
    {

        template
        <
            typename,
            typename,
            typename = void
        >
        struct endian_swap;


        //======================================================================
        // specialization for from == to (no byte swap)
        template
        <
            typename from_endian,
            typename to_endian
        >
        struct endian_swap
        <
            from_endian,
            to_endian,
            typename std::enable_if
            <
                std::is_same
                <
                    from_endian,
                    to_endian
                >::value
            >::type
        >
        {
            template <typename data_type>
            inline data_type operator()
            (
                data_type input
            ) const
            {
                return input;
            }
        };


        //======================================================================
        // specialization for from != to (do byte swap)
        template
        <
            typename from_endian,
            typename to_endian
        >
        struct endian_swap
        <
            from_endian,
            to_endian,
            typename std::enable_if
            <
                !std::is_same
                <
                    from_endian,
                    to_endian
                >::value
            >::type
        >
        {
            template <typename data_type>
            inline data_type operator()
            (
                data_type input
            ) const
            {
                return byte_swap(input);
            }
        };

    }


    //==========================================================================
    // static
    // do a byte swap from one endian to another as speicified
    template
    <
        typename from_endian,
        typename to_endian,
        typename data_type
    >
    static inline data_type endian_swap
    (
        data_type input
    )
    {
        return maniscalco::impl::endian_swap<from_endian, to_endian>()(input);
    }

}
