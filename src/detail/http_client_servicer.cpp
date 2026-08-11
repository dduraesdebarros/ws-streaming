#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <iostream>
#include <string>
#include <utility>

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/version.hpp>
#include <boost/beast/core/async_base.hpp>
#include <boost/beast/core/error.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/message_generator.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/optional/optional.hpp>
#include <boost/system/error_code.hpp>

#include <ws-streaming/detail/http_client_servicer.hpp>
#include <ws-streaming/detail/websocket_protocol.hpp>

using namespace std::chrono_literals;
using namespace std::placeholders;

wss::detail::http_client_servicer::http_client_servicer(
        boost::asio::ip::tcp::socket&& socket)
    : stream(std::move(socket))
{
}

void wss::detail::http_client_servicer::run()
{
    do_read();
}

void wss::detail::http_client_servicer::stop()
{
    boost::asio::post(
        stream.get_executor(),
        [self_weak = weak_from_this()]()
        {
            if (auto self = self_weak.lock())
                self->close();
        });
}

void wss::detail::http_client_servicer::do_read()
{
    stream.expires_after(30s);

    boost::beast::http::async_read(
        stream,
        buffer,
        req = {},
        [self_weak = weak_from_this()](const boost::system::error_code& ec, std::size_t bytes_transferred)
        {
            if (auto self = self_weak.lock())
                self->finish_read(ec, bytes_transferred);
        });
}

void wss::detail::http_client_servicer::do_write(
    boost::beast::http::message_generator&& msg,
    response_actions action)
{
    boost::beast::async_write(
        stream,
        std::move(msg),
        [self_weak = weak_from_this(), action](const boost::system::error_code& ec, std::size_t bytes_transferred)
        {
            if (auto self = self_weak.lock())
                self->finish_write(action, ec, bytes_transferred);
        });
}

void wss::detail::http_client_servicer::finish_read(
    const boost::system::error_code& ec,
    std::size_t /*bytes_transferred*/)
{
    if (ec)
        return close(ec);

    auto log_line = [](const std::string& message)
    {
        std::clog << "[DanielDebug] " << message << '\n';
    };

    log_line(
        std::string("http rx method=")
        + std::string(req.method_string())
        + " target=" + std::string(req.target())
        + " body_size=" + std::to_string(req.body().size()));

    auto get_header = [&](boost::beast::http::field field)
    {
        auto it = std::find_if(
            req.begin(),
            req.end(),
            [&](const auto& header)
            {
                return header.name() == field;
            });

        return it == req.end() ? "" : it->value();
    };

    auto key = get_header(boost::beast::http::field::sec_websocket_key);

    if (get_header(boost::beast::http::field::upgrade) == "websocket" && !key.empty())
    {
        log_line("http path=websocket-upgrade");

        auto response_key = detail::websocket_protocol::get_response_key(key);

        boost::beast::http::response<boost::beast::http::string_body> res(
            boost::beast::http::status::switching_protocols,
            req.version());

        res.set(
            boost::beast::http::field::server,
            "ws-streaming/" WS_STREAMING_VERSION_MAJOR
                "." WS_STREAMING_VERSION_MINOR
                "." WS_STREAMING_VERSION_PATCH
                " " BOOST_BEAST_VERSION_STRING);

        res.set(boost::beast::http::field::connection, "Upgrade");
        res.set(boost::beast::http::field::upgrade, "websocket");
        res.set(boost::beast::http::field::sec_websocket_accept, response_key);

        do_response(res);
    }

    else if (req.method() == boost::beast::http::verb::post)
    {
        log_line("http path=command-post");

        nlohmann::json request_json;

        try
        {
            request_json = nlohmann::json::parse(req.body());
        }

        catch (const nlohmann::json::exception& ex)
        {
            return do_response(
                req,
                boost::beast::http::status::internal_server_error,
                { { "code", -32700 }, { "message", ex.what() } });
        }

        if (!request_json.is_object()
                || !request_json.contains("method")
                || !request_json["method"].is_string())
            return do_response(
                req,
                boost::beast::http::status::bad_request,
                { { "code", -32700 }, { "message", "Request object is invalid" } });

        log_line(
            std::string("jsonrpc method=")
            + request_json["method"].get<std::string>()
            + " params_type="
            + (request_json.contains("params")
                ? request_json["params"].type_name()
                : "null"));

        boost::optional<nlohmann::json> response_json;

        try
        {
            response_json = on_command_interface_request(
                request_json["method"],
                request_json.contains("params")
                    ? request_json["params"]
                    : nlohmann::json{nullptr});
        }

        catch (const std::exception& ex)
        {
            return do_response(
                req,
                boost::beast::http::status::internal_server_error,
                { { "code", -32700 }, { "message", ex.what() } });
        }

        if (!response_json.has_value())
            return do_response(
                req,
                boost::beast::http::status::internal_server_error,
                { { "code", -32700 }, { "message", "No connected slot" } });

        log_line(
            std::string("command result type=")
            + response_json.value().type_name()
            + " body=" + response_json.value().dump());


        return do_response(
            req,
            boost::beast::http::status::ok,
            response_json.value());
    }

    else if (req.method() == boost::beast::http::verb::options)
    {
        log_line("http path=options");

        return do_response(
            req,
            boost::beast::http::status::no_content,
            nullptr);
    }

    else
    {
        boost::beast::http::response<boost::beast::http::string_body> res(
            boost::beast::http::status::bad_request,
            req.version());

        res.set(
            boost::beast::http::field::server,
            "ws-streaming/" WS_STREAMING_VERSION_MAJOR
                "." WS_STREAMING_VERSION_MINOR
                "." WS_STREAMING_VERSION_PATCH
                " " BOOST_BEAST_VERSION_STRING);

        res.keep_alive(req.keep_alive());
        res.set(boost::beast::http::field::access_control_allow_headers, "*");
        res.set(boost::beast::http::field::access_control_allow_origin, "*");
        res.prepare_payload();

        do_write(
            std::move(res),
            req.keep_alive()
                ? response_actions::keepalive
                : response_actions::close);
    }
}

void wss::detail::http_client_servicer::finish_write(
    response_actions action,
    const boost::beast::error_code& ec,
    std::size_t /*bytes_transferred*/)
{
    if (ec)
        return close(ec);

    switch (action)
    {
        case response_actions::keepalive:
            return do_read();

        case response_actions::upgrade:
        {
            auto socket = stream.release_socket();
            return on_websocket_upgrade(socket);
        }

        default:
            return close();
    }
}

void wss::detail::http_client_servicer::do_response(
    const boost::beast::http::request<boost::beast::http::string_body>& req,
    boost::beast::http::status status,
    const nlohmann::json& response_json)
{
    boost::beast::http::response<boost::beast::http::string_body> res(
        status,
        req.version());

    res.keep_alive(req.keep_alive());
    res.set(boost::beast::http::field::access_control_allow_headers, "*");
    res.set(boost::beast::http::field::access_control_allow_origin, "*");

    auto log_line = [](const std::string& message)
    {
        std::clog << "[DanielDebug] " << message << '\n';
    };

    if (!response_json.is_null())
    {
        bool is_success;

        log_line(
            std::string("compat check status=")
            + std::to_string(static_cast<unsigned>(status))
            + " json_type=" + response_json.type_name()
            + " json_body=" + response_json.dump());

        if (response_json.is_array())
        {
            is_success = true;
            for (const auto& entry : response_json)
            {
                if (!entry.is_boolean() || entry != true)
                {
                    is_success = false;
                    break;
                }
            }
        }

        else if (response_json.is_boolean())
        {
            is_success = response_json == true;
        }

        else
        {
            is_success = false;
        }

        if (is_success)
        {
            res.set(boost::beast::http::field::content_type, "text/plain");
            res.body() = "Succeeded";
            log_line("compat branch=Succeeded");
        }

        else
        {
            res.set(boost::beast::http::field::content_type, "application/json");
            res.body() = response_json.dump();
            log_line("compat branch=application/json");
        }
    }

    log_line(
        std::string("tx pre-prepare status=")
        + std::to_string(static_cast<unsigned>(status))
        + " content_type="
        + (res.find(boost::beast::http::field::content_type) != res.end()
            ? std::string(res.find(boost::beast::http::field::content_type)->value())
            : "<unset>")
        + " body=" + res.body());

    do_response(res);
}

template <typename Body>
void wss::detail::http_client_servicer::do_response(
    boost::beast::http::response<Body>& response)
{
    response.set(
        boost::beast::http::field::server,
        "ws-streaming/" WS_STREAMING_VERSION_MAJOR
            "." WS_STREAMING_VERSION_MINOR
            "." WS_STREAMING_VERSION_PATCH
            " " BOOST_BEAST_VERSION_STRING);

    response.prepare_payload();

    do_write(
        std::move(response),
        response.result() == boost::beast::http::status::switching_protocols
            ? response_actions::upgrade
            : response.keep_alive()
                ? response_actions::keepalive
                : response_actions::close);
}

void wss::detail::http_client_servicer::close(
    const boost::system::error_code& ec)
{
    if (ec == boost::beast::error::timeout)
        return on_closed(ec);

    if (!stream.socket().is_open())
        return;

    boost::beast::error_code shutdown_ec;
    stream.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_send, shutdown_ec);
    stream.close();

    on_closed(ec);
}
