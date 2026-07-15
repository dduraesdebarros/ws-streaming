#pragma once

#include <type_traits>
#include <boost/asio/ssl/stream.hpp>

namespace wss::detail
{
    template <typename T>
    struct is_ssl_stream : std::false_type {};

    template <typename NextLayer>
    struct is_ssl_stream<boost::asio::ssl::stream<NextLayer>> : std::true_type {};

    template <typename T>
    inline constexpr bool is_ssl_stream_v = is_ssl_stream<T>::value;
}
