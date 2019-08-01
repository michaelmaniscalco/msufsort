#pragma once


namespace maniscalco
{

    struct big_endian_type;
    struct little_endian_type;

    using network_order_type = big_endian_type;
    using host_order_type = little_endian_type;
   //    using host_order_type = big_endian_type;
}
