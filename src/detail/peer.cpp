#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/socket_base.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/system/error_code.hpp>

#include <nlohmann/json.hpp>

#include <ws-streaming/detail/peer.hpp>
#include <ws-streaming/detail/streaming_protocol.hpp>
#include <ws-streaming/detail/websocket_protocol.hpp>

using namespace std::placeholders;

wss::detail::peer::peer(
        boost::asio::ip::tcp::socket&& socket,
        bool /*is_client*/,
        bool use_tcp_protocol,
        std::size_t rx_buffer_size,
        std::size_t tx_buffer_size)
    : base_peer(use_tcp_protocol, rx_buffer_size)
    , _socket{std::move(socket)}
    , _tx_buffer(tx_buffer_size)
{
    _socket.non_blocking(true);
    set_send_buffer_size(tx_buffer_size);
}

void wss::detail::peer::send_metadata(
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

void wss::detail::peer::set_send_buffer_size(std::size_t size)
{
    // Clamp the requested tx buffer size to the largest value we can store in an int.
    if (size > std::numeric_limits<int>::max())
        size = std::numeric_limits<int>::max();

    // Ask the operating system to hold as much as possible.
    boost::system::error_code ec;
    auto option = boost::asio::socket_base::send_buffer_size{static_cast<int>(size)};
    _socket.set_option(option, ec);
}

void wss::detail::peer::do_read()
{
    _socket.async_wait(
        boost::asio::socket_base::wait_read,
        [self_weak = weak_from_this()](const boost::system::error_code& ec)
        {
            if (auto self = std::static_pointer_cast<peer>(self_weak.lock()))
                self->finish_wait_rx(ec);
        });
}

void wss::detail::peer::do_wait_tx()
{
    _socket.async_wait(
        boost::asio::socket_base::wait_write,
        [self_weak = weak_from_this()](const boost::system::error_code& ec)
        {
            if (auto self = std::static_pointer_cast<peer>(self_weak.lock()))
                self->finish_wait_tx(ec);
        });

    _waiting_tx = true;
}

void wss::detail::peer::finish_wait_rx(const boost::system::error_code& wait_ec)
{
    boost::system::error_code receive_ec;

    // Was there an error waiting for the socket to become readable?
    if (wait_ec)
        return close(wait_ec);

    // Since the socket is readable, read as much as we can from it.
    std::size_t bytes_received = _socket.receive(
        boost::asio::buffer(
            _rx_buffer.data() + _rx_buffer_bytes,
            _rx_buffer.size() - _rx_buffer_bytes),
        0,
        receive_ec);

    if (bytes_received == 0)
        return close({});

    // Was there a genuine error reading from the socket?
    if (receive_ec && receive_ec != boost::asio::error::would_block)
        return close(receive_ec);

    _rx_buffer_bytes += bytes_received;

    process_buffer();
}

void wss::detail::peer::finish_wait_tx(const boost::system::error_code& wait_ec)
{
    boost::system::error_code send_ec;
    _waiting_tx = false;

    // Was there an error waiting for the socket to become writeable?
    if (wait_ec)
        return close(wait_ec);

    // Since the socket is writeable, write as much as we can to it.
    std::size_t bytes_sent = _socket.send(
        boost::asio::buffer(_tx_buffer.data(), _tx_buffer_bytes),
        0,
        send_ec);

    // Was there a genuine error writing to the socket?
    if (send_ec && send_ec != boost::asio::error::would_block)
        return close(send_ec);

    // If we sent all the data in our buffer, we can stop now.
    if (bytes_sent)
        std::memmove(
            _tx_buffer.data(),
            &_tx_buffer[bytes_sent],
            _tx_buffer_bytes - bytes_sent);
    _tx_buffer_bytes -= bytes_sent;

    if (_shutdown_after)
    {
        _shutdown_after -= std::min(bytes_sent, _shutdown_after);
        if (_shutdown_after == 0)
            return close();
    }

    if (bytes_sent < _tx_buffer_bytes)
        do_wait_tx();
}

void wss::detail::peer::shutdown_and_close()
{
    boost::system::error_code close_ec;
    _socket.shutdown(_socket.shutdown_both, close_ec);
    _socket.close(close_ec);
}

void wss::detail::peer::send_control_frame(
    unsigned opcode,
    const boost::asio::const_buffer& payload,
    std::size_t size,
    bool shutdown_after)
{
    send_websocket_frame(opcode, payload, size, shutdown_after);
}
