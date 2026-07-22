#pragma once

#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/system/error_code.hpp>

#include <ws-streaming/connection.hpp>
#include <ws-streaming/detail/http_client.hpp>
#include <ws-streaming/detail/url.hpp>

namespace wss
{
    /**
     * Asynchronously establishes a WebSocket Streaming @ref connection by making an
     * HTTP/WebSocket request to a remote @ref server. The application calls async_connect() with
     * a WebSocket URL. When the WebSocket connection is established, or an error occurs, the
     * completion function passed to async_connect() is called with the corresponding error code
     * and, if successful, a constructed @ref connection object.
     *
     * Connections can be unencrypted (`ws://` URLs) or encrypted with TLS (`wss://` URLs). To use
     * TLS, call enable_tls() to configure the certificate files to use before calling
     * async_connect().
     *
     * A client object can handle multiple async_connect() calls, but only one connection attempt
     * at a time may be in progress. Connection attempts can be canceled by calling cancel(); the
     * completion handler will then be called with the error code
     * boost::asio::error::operation_aborted.
     *
     * @subsubsection Example
     * @include client-usage.cpp
     */
    class client
    {
        public:

            /**
             * Constructs a client object. Asynchronous socket operations will be dispatched using
             * the specified execution context.
             *
             * @param executor An execution context to use for asynchronous I/O operations.
             */
            client(boost::asio::any_io_executor executor);

            /**
             * Enables TLS for `wss://` connections and configures the certificate files to use.
             * A CA file is required to verify the server. If a `wss://` URL is used without calling
             * this function first, a TLS context is built lazily from these same parameters (all
             * empty by default), which likewise requires a CA file: connecting over `wss://`
             * without a CA file configured raises std::invalid_argument.
             *
             * @include tls-usage.cpp
             *
             * @param ca_file Path to a PEM file of trusted CA certificates used to verify the
             *     server. Required: if empty, std::invalid_argument is thrown.
             * @param client_cert_file Path to the client certificate chain (PEM), for mutual TLS.
             *     Optional, but if set client_key_file must also be set.
             * @param client_key_file Path to the client private key (PEM), for mutual TLS.
             *     Optional, but if set client_cert_file must also be set.
             *
             * @throws std::invalid_argument ca_file is empty, or exactly one of client_cert_file
             *     and client_key_file is set.
             * @throws boost::system::system_error A certificate or key file could not be loaded.
             */
            void enable_tls(
                const std::string& ca_file,
                const std::string& client_cert_file = {},
                const std::string& client_key_file = {});

            /**
             * Asynchronously connects to a remote server. An HTTP GET request is made to
             * establish the WebSocket connection.
             *
             * The scheme of the URL selects the transport: `ws://` for an unencrypted WebSocket
             * connection, `wss://` for a TLS-encrypted one, and any other scheme for a direct TCP
             * protocol connection. If the URL does not specify a port number, the default port
             * for the scheme is used: 7414 for `ws://`, 7415 for `wss://`, and 7411 for direct
             * TCP.
             *
             * @param url The WebSocket URL of the remote server.
             * @param handler A completion handler to call when the operation is complete. This
             *     handler receives either a nonzero error code, or a std::shared_ptr holding a
             *     constructed @ref connection object on which connection::run() has been called.
             *     The handler is guaranteed to be called exactly once, and to be dispatched using
             *     the execution context passed to the constructor.
             *
             * @throws std::invalid_argument A `wss://` URL was given, but TLS has not been
             *     configured by a successful call to enable_tls(). The completion handler is not
             *     called in this case.
             */
            void async_connect(
                std::string_view url,
                std::function<
                    void(
                        const boost::system::error_code& ec,
                        wss::connection_ptr connection)
                > handler);

            /**
             * Cancels a pending connection attempt, if any. The handler passed to async_connect()
             * will be called with the error code boost::asio::error::operation_aborted, if it has
             * not already been called. Note that it is possible a successful connection attempt
             * has already been scheduled with the execution context, resulting in the handler
             * being called with a successful connection even after calling this function.
             * Cancellation guarantees only that some call to the completion handler, successful
             * or otherwise, will be scheduled with the execution context.
             */
            void cancel();

        private:

            boost::beast::http::request<boost::beast::http::string_body> create_request(
                const detail::url& url);

            std::string get_random_key();

            // Lazily builds the TLS context and https_client on first use, if not already built by
            // enable_tls(). Returns the https_client to use for a wss:// connection.
            const std::shared_ptr<detail::https_client>& ensure_https_client();

        private:

            std::shared_ptr<detail::http_client> _http_client;
            boost::asio::any_io_executor _executor;

            std::unique_ptr<boost::asio::ssl::context> _ssl_context;
            std::shared_ptr<detail::https_client> _https_client;
            std::string _ca_file;
            std::string _cert_file;
            std::string _key_file;
    };
}
