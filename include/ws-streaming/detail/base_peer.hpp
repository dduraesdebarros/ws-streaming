#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio/buffer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/signals2/signal.hpp>
#include <boost/system/error_code.hpp>

#include <nlohmann/json.hpp>

#include <ws-streaming/detail/streaming_protocol.hpp>
#include <ws-streaming/detail/websocket_protocol.hpp>

namespace wss::detail
{
    /**
     * Base class for a WebSocket Streaming Protocol connection to a remote peer.
     *
     * This is the surface that wss::connection talks to. Two concrete implementations exist:
     *   - wss::detail::peer      - a plaintext TCP transport.
     *   - wss::detail::peer_tls  - a TLS-encrypted transport built on boost::asio::ssl::stream.
     *
     * The framing logic is transport-agnostic and lives here: the entire receive path (WebSocket
     * frame decoding, streaming-protocol packet assembly, metadata decoding) and the connection
     * lifecycle (run(), stop(), close()) are implemented once in this base class. The concrete
     * classes supply only the transport primitives.
     * The transmit path (send_data(), send_metadata()) is deliberately left to the concrete
     * classes, because it is coupled to a transport-specific write strategy: the plaintext peer
     * performs zero-copy scatter-gather sends, whereas the TLS peer must materialize and serialize
     * each frame through the OpenSSL engine.
     *
     * The three signals below are owned here so that connection can connect to them without
     * knowing the concrete transport type.
     *
     * All member functions must be invoked from the socket's execution context, which must be an
     * explicit or implicit strand. This class is not thread-safe.
     */
    class base_peer : public std::enable_shared_from_this<base_peer>
    {
        public:

            virtual ~base_peer() = default;

            /**
             * Activates the peer by starting asynchronous I/O operations using the socket's
             * execution context.
             */
            void run();

            /**
             * Activates the peer, processing the specified data as if it had been received from the
             * socket. Useful when an earlier HTTP layer inadvertently buffered streaming data.
             *
             * @param data A pointer to the data to process.
             * @param size The number of bytes pointed to by @p data.
             */
            void run(const void *data, std::size_t size);

            /**
             * Closes the connection, canceling any pending asynchronous socket operations. The
             * on_closed signal will be raised later from the execution context, unless it has
             * already been raised.
             */
            void stop();

            /**
             * Asynchronously sends signal data to the remote peer.
             *
             * @param signo The signal number to which the data applies.
             * @param data A Boost.Asio buffer describing the data to send.
             */
            virtual void send_data(
                unsigned signo,
                const boost::asio::const_buffer& data) = 0;

            /**
             * Asynchronously sends JSON-RPC metadata to the remote peer.
             *
             * @param signo The signal number to which the metadata applies, or zero for global
             *     metadata.
             * @param method The metadata method name.
             * @param params A JSON value containing the metadata parameters.
             */
            virtual void send_metadata(
                unsigned signo,
                const std::string& method,
                const nlohmann::json& params) = 0;

            /**
             * Gets the underlying TCP socket
             *
             * @return The underlying TCP socket.
             */
            virtual boost::asio::ip::tcp::socket& socket() = 0;

            /**
             * A signal raised when a WebSocket Streaming Protocol data packet is received.
             * The signal is raised from the execution context of the socket.
             *
             * @param signo The signal number to which the data applies.
             * @param data A pointer to the payload data.
             * @param size The number of payload data bytes pointed to by @p data.
             *
             * @throws ... Connected slots should not throw exceptions. If they do, they will
             *     propagate out to the execution context. This can result in an unhandled
             *     exception on a thread and terminate the process.
             */
            boost::signals2::signal<
                void(
                    unsigned signo,
                    const std::uint8_t *data,
                    std::size_t size)
            > on_data_received;

            /**
             * A signal raised when a WebSocket Streaming Protocol metadata packet is received.
             * The signal is raised from the execution context of the socket.
             *
             * @param signo The signal number to which the metadata applies, or zero for global
             *     metadata.
             * @param method The metadata method name.
             * @param params A JSON value containing the metadata parameters.
             *
             * @throws ... Connected slots should not throw exceptions. If they do, they will
             *     propagate out to the execution context. This can result in an unhandled
             *     exception on a thread and terminate the process.
             */
            boost::signals2::signal<
                void(
                    unsigned signo,
                    const std::string& method,
                    const nlohmann::json& params)
            > on_metadata_received;

            /**
             * A signal raised when the connection is closed. This can occur due to an error, or
             * when stop() is called. The signal is raised from the execution context of the
             * socket. Note that this may occur even after a caller has released its
             * std::shared_ptr reference to a peer object.
             *
             * @param ec The error code of the error, if any, that caused the connection to be
             *     closed.
             *
             * @throws ... Connected slots should not throw exceptions. If they do, they will
             *     propagate out to the execution context. This can result in an unhandled
             *     exception on a thread and terminate the process.
             */
            boost::signals2::signal<
                void(const boost::system::error_code& ec)
            > on_closed;

        protected:

            /**
             * Constructs the shared state of a peer.
             *
             * @param use_tcp_protocol True to use the direct TCP protocol instead of the
             *     WebSocket-based protocol.
             * @param rx_buffer_size The size of the fixed receive buffer.
             */
            base_peer(bool use_tcp_protocol, std::size_t rx_buffer_size);

            /**
             * Starts one asynchronous read into the receive buffer. The completion handler must
             * update _rx_buffer_bytes and call process_buffer() (or close() on error).
             */
            virtual void do_read() = 0;

            /**
             * Shuts down and closes the underlying socket or stream. Called once from close().
             */
            virtual void shutdown_and_close() = 0;

            /**
             * Transmits a WebSocket control frame. Used by the receive path to answer an incoming
             * CLOSE frame (with an empty payload and @p shutdown_after set) and to answer a PING
             * frame with a PONG echoing its payload.
             *
             * @param opcode The WebSocket opcode.
             * @param payload The frame payload.
             * @param size The number of payload bytes.
             * @param shutdown_after True to close the connection once the frame has been sent.
             */
            virtual void send_control_frame(
                unsigned opcode,
                const boost::asio::const_buffer& payload,
                std::size_t size,
                bool shutdown_after) = 0;

            void process_buffer();
            void process_buffer_tcp();
            void process_buffer_ws();

            void process_websocket_frame(
                const detail::websocket_protocol::decoded_header& header,
                std::uint8_t *data,
                std::size_t size,
                boost::system::error_code& ec);

            std::size_t process_packet(const std::uint8_t *data, std::size_t size);
            void process_data_packet(unsigned signo, const std::uint8_t *data, std::size_t size);
            void process_metadata_packet(unsigned signo, const std::uint8_t *data, std::size_t size);

            void close(const boost::system::error_code& ec = {});

            bool _use_tcp_protocol = false;
            bool _is_closed = false;

            std::vector<std::uint8_t> _rx_buffer;
            std::size_t _rx_buffer_bytes = 0;
    };
}
