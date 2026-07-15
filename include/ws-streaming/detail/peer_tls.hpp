#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio/buffer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core/buffers_cat.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/system/error_code.hpp>

#include <nlohmann/json.hpp>

#include <ws-streaming/detail/base_peer.hpp>
#include <ws-streaming/detail/streaming_protocol.hpp>
#include <ws-streaming/detail/websocket_protocol.hpp>

namespace wss::detail
{
    /**
     * Implements the transport layer of a WebSocket Streaming Protocol connection to a remote
     * peer over a TLS-encrypted stream. This is the TLS counterpart of wss::detail::peer:
     * it speaks exactly the same WebSocket Streaming Protocol framing, but exchanges bytes over a
     * boost::asio::ssl::stream instead of a plaintext TCP socket.
     *
     * Peer objects are constructed with, and take ownership of, an already-connected and already-
     * handshaken ssl::stream. The TLS handshake is performed by the HTTP layer before the stream
     * is handed down. Peer objects must always be managed by a std::shared_ptr, following the
     * normal Boost.Asio pattern. All member functions must be invoked from the stream's execution
     * context, which must be an explicit or implicit strand. This class is not thread-safe.
     */
    class peer_tls : public base_peer
    {
        public:

            using ssl_stream = boost::asio::ssl::stream<boost::beast::tcp_stream>;

            /**
             * Constructs a peer_tls object, taking ownership of the specified TLS stream. The
             * stream must already be connected and its TLS handshake must already be complete. No
             * asynchronous operations are started until run() is called.
             *
             * @param stream A connected, handshaken ssl::stream, which the constructed object
             *     takes ownership of.
             * @param is_client True if this object should act as a client.
             * @param use_tcp_protocol True to use the direct TCP protocol instead of the
             *     WebSocket-based protocol.
             * @param rx_buffer_size The desired size of the receive buffer. Sets an upper bound on
             *     the size of frames the peer can receive.
             * @param tx_buffer_size The desired maximum number of bytes that may be buffered in
             *     the user-space transmit queue while waiting for a slow peer. If exceeded, the
             *     connection is closed.
             */
            peer_tls(
                ssl_stream&& stream,
                bool is_client,
                bool use_tcp_protocol = false,
                std::size_t rx_buffer_size = 1024 * 1024,
                std::size_t tx_buffer_size = 32 * 1024 * 1024);

            void send_data(
                unsigned signo,
                const boost::asio::const_buffer& data) override
            {
                send_packet(
                    signo,
                    detail::streaming_protocol::packet_type::DATA,
                    data,
                    std::nullopt);
            }

            void send_metadata(
                unsigned signo,
                const std::string& method,
                const nlohmann::json& params) override;

            boost::asio::ip::tcp::socket& socket() override
            {
                return _stream.next_layer().socket();
            }

        private:

            void do_read() override;
            void shutdown_and_close() override;
            void send_control_frame(
                unsigned opcode,
                const boost::asio::const_buffer& payload,
                std::size_t size,
                bool shutdown_after) override;

            void finish_read(
                const boost::system::error_code& ec,
                std::size_t bytes_transferred);

            void do_write();
            void finish_write(
                const boost::system::error_code& ec,
                std::size_t bytes_transferred);

            template <typename ConstBufferSequence>
            void send_packet(
                unsigned signo,
                unsigned type,
                const ConstBufferSequence& payload,
                const std::optional<std::size_t>& payload_size)
            {
                std::array<std::uint8_t, detail::streaming_protocol::MAX_HEADER_SIZE> streaming_header;

                std::size_t calculated_payload_size
                    = payload_size.has_value()
                        ? payload_size.value()
                        : boost::asio::buffer_size(payload);

                auto streaming_header_size = detail::streaming_protocol::generate_header(
                    streaming_header.data(),
                    signo,
                    type,
                    calculated_payload_size);

                if (_use_tcp_protocol)
                    write(
                        boost::beast::buffers_cat(
                            boost::asio::buffer(
                                streaming_header.data(),
                                streaming_header_size),
                            payload),
                        streaming_header_size + calculated_payload_size,
                        false);
                else
                    send_websocket_frame(
                        detail::websocket_protocol::opcodes::BINARY,
                        boost::beast::buffers_cat(
                            boost::asio::buffer(
                                streaming_header.data(),
                                streaming_header_size),
                            payload),
                        streaming_header_size + calculated_payload_size);
            }

            template <typename ConstBufferSequence>
            void send_websocket_frame(
                unsigned opcode,
                const ConstBufferSequence& payload,
                const std::optional<std::size_t>& payload_size,
                bool do_shutdown_after = false)
            {
                std::array<std::uint8_t, detail::websocket_protocol::MAX_HEADER_SIZE> ws_header;

                std::size_t calculated_payload_size
                    = payload_size.has_value()
                        ? payload_size.value()
                        : boost::asio::buffer_size(payload);

                auto ws_header_size = detail::websocket_protocol::generate_header(
                    ws_header.data(),
                    opcode,
                    detail::websocket_protocol::flags::FIN,
                    calculated_payload_size);

                write(
                    boost::beast::buffers_cat(
                        boost::asio::buffer(
                            ws_header.data(),
                            ws_header_size),
                        payload),
                    ws_header_size + calculated_payload_size,
                    do_shutdown_after);
            }

            // Unlike wss::detail::peer, which writes the buffer sequence directly to the socket (zero-copy),
            // the TLS path must materialize each frame into an owned contiguous
            // buffer and enqueue it for serialized transmission through the OpenSSL engine
            template <typename ConstBufferSequence>
            void write(
                const ConstBufferSequence& buffers,
                const std::optional<std::size_t>& size,
                bool do_shutdown_after)
            {
                if (_is_closed)
                    return;

                std::size_t calculated_size
                    = size.has_value()
                        ? size.value()
                        : boost::asio::buffer_size(buffers);

                auto message = std::make_shared<std::vector<std::uint8_t>>(calculated_size);
                boost::asio::buffer_copy(
                    boost::asio::buffer(*message),
                    buffers);

                _tx_queued_bytes += calculated_size;
                _tx_queue.push_back(tx_message{std::move(message), do_shutdown_after});

                // Bound the user-space transmit queue
                // if a slow peer lets it grow past the configured limit,
                // close the connection rather than buffer without bound
                if (_tx_queued_bytes > _tx_buffer_limit)
                    return close(boost::asio::error::no_buffer_space);

                if (!_write_in_flight)
                    do_write();
            }

        private:

            struct tx_message
            {
                std::shared_ptr<std::vector<std::uint8_t>> data;
                bool shutdown_after = false;
            };

            ssl_stream _stream;

            std::deque<tx_message> _tx_queue;
            std::size_t _tx_queued_bytes = 0;
            std::size_t _tx_buffer_limit = 0;
            bool _write_in_flight = false;
    };
}
