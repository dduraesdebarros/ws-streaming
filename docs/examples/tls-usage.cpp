#include <ws-streaming/ws-streaming.hpp>

wss::client client{ioc};

// Verify the server against the CA certificates in ca.pem. To authenticate this
// client to the server as well, also pass its certificate and private key files.
client.enable_tls("ca.pem");

client.async_connect(
    "wss://localhost:7415",
    [](const boost::system::error_code& ec, connection_ptr connection)
    {
        if (ec) // error occurred
            return;

        // encrypted WebSocket connection is successfully established
    });
