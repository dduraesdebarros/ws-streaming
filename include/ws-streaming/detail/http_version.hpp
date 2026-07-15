#pragma once

namespace wss::detail
{
    /**
     * Returns the product identification string used in HTTP User-Agent (client) and Server
     * (server) headers, e.g. "ws-streaming/3.0.7 Boost.Beast/...".
     */
    const char* http_product_string();
}
