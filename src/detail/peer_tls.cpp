#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/system/error_code.hpp>

#include <nlohmann/json.hpp>

#include <ws-streaming/detail/peer_tls.hpp>
#include <ws-streaming/detail/streaming_protocol.hpp>
#include <ws-streaming/detail/websocket_protocol.hpp>

using namespace std::placeholders;

wss::detail::peer_tls::peer_tls(
        ssl_stream&& stream,
        bool /*is_client*/,
        bool use_tcp_protocol,
        std::size_t rx_buffer_size,
        std::size_t tx_buffer_size)
    : base_peer(use_tcp_protocol, rx_buffer_size)
    , _stream{std::move(stream)}
    , _tx_buffer_limit(tx_buffer_size)
{
    // Note: unlike wss::detail::peer, the underlying socket is left in the asynchronous mode
    // managed by Boost.Asio. async_read_some/async_write drive the OpenSSL engine; we must not
    // put the socket into manual non-blocking mode.
}

void wss::detail::peer_tls::send_metadata(
    unsigned signo,
    const std::string& method,
    const nlohmann::json& params)
{
    std::uint32_t encoding = detail::streaming_protocol::metadata_encoding::MSGPACK;

    auto payload = nlohmann::json::to_msgpack({
        {"method", method},
        {"params", params}
    });

    std::array<boost::asio::const_buffer, 2> buffers =
    {
        boost::asio::buffer(&encoding, sizeof(encoding)),
        boost::asio::buffer(payload),
    };

    send_packet(
        signo,
        detail::streaming_protocol::packet_type::METADATA,
        buffers,
        sizeof(encoding) + payload.size());
}

void wss::detail::peer_tls::do_read()
{
    _stream.async_read_some(
        boost::asio::buffer(
            _rx_buffer.data() + _rx_buffer_bytes,
            _rx_buffer.size() - _rx_buffer_bytes),
        [self_weak = weak_from_this()](const boost::system::error_code& ec, std::size_t bytes_transferred)
        {
            if (auto self = std::static_pointer_cast<peer_tls>(self_weak.lock()))
                self->finish_read(ec, bytes_transferred);
        });
}

void wss::detail::peer_tls::finish_read(
    const boost::system::error_code& ec,
    std::size_t bytes_transferred)
{
    if (ec)
        return close(ec);

    if (bytes_transferred == 0)
        return close({});

    _rx_buffer_bytes += bytes_transferred;

    process_buffer();
}

void wss::detail::peer_tls::do_write()
{
    _write_in_flight = true;

    boost::asio::async_write(
        _stream,
        boost::asio::buffer(*_tx_queue.front().data),
        [self_weak = weak_from_this()](const boost::system::error_code& ec, std::size_t bytes_transferred)
        {
            if (auto self = std::static_pointer_cast<peer_tls>(self_weak.lock()))
                self->finish_write(ec, bytes_transferred);
        });
}

void wss::detail::peer_tls::finish_write(
    const boost::system::error_code& ec,
    std::size_t /*bytes_transferred*/)
{
    _write_in_flight = false;

    if (ec)
        return close(ec);

    tx_message entry = std::move(_tx_queue.front());
    _tx_queue.pop_front();
    _tx_queued_bytes -= entry.data->size();

    if (entry.shutdown_after)
        return close();

    if (!_tx_queue.empty())
        do_write();
}

void wss::detail::peer_tls::shutdown_and_close()
{
    boost::system::error_code close_ec;
    _stream.lowest_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both, close_ec);
    _stream.lowest_layer().close(close_ec);
}

void wss::detail::peer_tls::send_control_frame(
    unsigned opcode,
    const boost::asio::const_buffer& payload,
    std::size_t size,
    bool shutdown_after)
{
    send_websocket_frame(opcode, payload, size, shutdown_after);
}
