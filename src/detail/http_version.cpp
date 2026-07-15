#include <boost/beast/version.hpp>

#include <ws-streaming/detail/http_client.hpp>
#include <ws-streaming/detail/http_version.hpp>

const char* wss::detail::http_product_string()
{
    return "ws-streaming/" WS_STREAMING_VERSION_MAJOR
        "." WS_STREAMING_VERSION_MINOR
        "." WS_STREAMING_VERSION_PATCH
        " " BOOST_BEAST_VERSION_STRING;
}
