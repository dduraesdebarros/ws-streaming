#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#include <boost/system/system_error.hpp>
#include <gtest/gtest.h>

#include <ws-streaming/detail/tls.hpp>
#include <ws-streaming/ws-streaming.hpp>

#define WS_STREAMING_TEST_SECRETS_DIR "secrets"

using namespace std::chrono_literals;

namespace
{

struct exchange_result
{
    bool handler_called = false;
    bool connect_failed = false;
    bool got_data = false;
};

class TlsTest : public ::testing::Test
{
protected:
    std::string path(const char* name) const
    {
        return (dir / name).string();
    }

    static bool samples_match(const std::vector<int64_t>& samples, const void* data, std::size_t count)
    {
        if (count != samples.size())
            return false;
        for (std::size_t i = 0; i < count; ++i)
        {
            if (samples[i] != *(static_cast<const int64_t*>(data) + i))
                return false;
        }
        return true;
    }

    static void start_subscription(wss::client& client,
                                   std::string url,
                                   const std::vector<int64_t>& samples,
                                   exchange_result& result,
                                   std::function<void()> finish,
                                   std::vector<boost::signals2::scoped_connection>& conns,
                                   wss::connection_ptr& held)
    {
        client.async_connect(std::move(url),
                             [&, finish = std::move(finish)](const boost::system::error_code& ec, wss::connection_ptr connection)
                             {
                                 result.handler_called = true;
                                 if (ec || !connection)
                                 {
                                     result.connect_failed = true;
                                     finish();
                                     return;
                                 }

                                 held = connection;
                                 conns.emplace_back(connection->on_available.connect(
                                     [&, finish](wss::remote_signal_ptr signal)
                                     {
                                         if (signal->id() != "/Channel1/Value")
                                             return;
                                         conns.emplace_back(signal->on_data_received.connect(
                                             [&, finish](std::int64_t, std::size_t sample_cnt, const void* data, std::size_t)
                                             {
                                                 if (samples_match(samples, data, sample_cnt))
                                                 {
                                                     result.got_data = true;
                                                     finish();
                                                 }
                                             }));
                                         signal->subscribe();
                                     }));
                             });
    }

    exchange_result run_exchange_url(const std::string& url,
                                     const std::function<void(wss::server&)>& configure_server,
                                     const std::function<void(wss::client&)>& configure_client)
    {
        boost::asio::io_context ioc{1};

        wss::local_signal time_signal{"/Time",
                                      wss::metadata_builder{"Time"}
                                          .data_type(wss::data_types::int64_t)
                                          .unit(wss::unit::seconds)
                                          .linear_rule(0, 1)
                                          .origin(wss::metadata::unix_epoch)
                                          .table("/Time")
                                          .build()};

        wss::local_signal value_signal{"/Channel1/Value",
                                       wss::metadata_builder{"Value"}
                                           .data_type(wss::data_types::int64_t)
                                           .unit(wss::unit::volts)
                                           .range(-100, 100)
                                           .table(time_signal.id())
                                           .build()};

        wss::server server{ioc.get_executor()};
        configure_server(server);
        server.add_local_signal(time_signal);
        server.add_local_signal(value_signal);
        server.run();

        const std::vector<int64_t> samples{-1, 0, 1, 5, 9, 78, 7, 2, -55, 20};

        exchange_result result;
        std::promise<void> done;
        auto done_future = done.get_future();
        std::atomic<bool> done_set{false};
        auto finish = [&]()
        {
            if (!done_set.exchange(true))
                done.set_value();
        };

        std::vector<boost::signals2::scoped_connection> conns;
        wss::connection_ptr held;

        wss::client client{ioc.get_executor()};
        configure_client(client);

        start_subscription(client, url, samples, result, finish, conns, held);

        std::thread io_thread{[&] { ioc.run(); }};

        std::atomic<bool> stop{false};
        std::thread publisher{[&]()
                              {
                                  std::int64_t t = 0;
                                  while (!stop)
                                  {
                                      value_signal.publish_data(t++, samples.size(), samples.data(), samples.size() * sizeof(int64_t));
                                      std::this_thread::sleep_for(20ms);
                                  }
                              }};

        done_future.wait_for(5s);

        stop = true;
        publisher.join();
        ioc.stop();
        io_thread.join();

        return result;
    }

    exchange_result
    run_exchange(uint16_t port, const std::string& server_ca_for_mtls, const std::function<void(wss::client&)>& configure_client)
    {
        return run_exchange_url(
            "wss://127.0.0.1:" + std::to_string(port),
            [this, port, server_ca_for_mtls](wss::server& server)
            { server.add_tls_listener(port, path("server.crt"), path("server.key"), server_ca_for_mtls); },
            configure_client);
    }

    static const std::filesystem::path dir;
};

const std::filesystem::path TlsTest::dir{WS_STREAMING_TEST_SECRETS_DIR};

// without mTLS
TEST_F(TlsTest, ServerAuthenticatedExchange)
{
    auto result = run_exchange(17415, /*mtls ca*/ {}, [&](wss::client& c) { c.enable_tls(path("ca.crt")); });

    EXPECT_TRUE(result.handler_called);
    EXPECT_FALSE(result.connect_failed);
    EXPECT_TRUE(result.got_data);
}

// with mTLS
TEST_F(TlsTest, MutualTlsExchange)
{
    auto result = run_exchange(17416,
                               /*mtls ca*/ path("ca.crt"),
                               [&](wss::client& c) { c.enable_tls(path("ca.crt"), path("client.crt"), path("client.key")); });

    EXPECT_TRUE(result.handler_called);
    EXPECT_FALSE(result.connect_failed);
    EXPECT_TRUE(result.got_data);
}

// wrong ca.crt
TEST_F(TlsTest, UntrustedServerRejected)
{
    auto result = run_exchange(17417, /*mtls ca*/ {}, [&](wss::client& c) { c.enable_tls(path("other-ca.crt")); });

    EXPECT_TRUE(result.connect_failed);
    EXPECT_FALSE(result.got_data);
}

// mTLS server must reject a client that presents no certificate (verify_fail_if_no_peer_cert)
TEST_F(TlsTest, MutualTlsRejectsClientWithoutCertificate)
{
    auto result = run_exchange(17418,
                               /*mtls ca*/ path("ca.crt"),
                               [&](wss::client& c) { c.enable_tls(path("ca.crt")); });

    EXPECT_TRUE(result.connect_failed);
    EXPECT_FALSE(result.got_data);
}

// A plaintext ws:// client must not be able to talk to a TLS-only listener
TEST_F(TlsTest, PlaintextClientRejectedByTlsListener)
{
    auto result = run_exchange_url(
        "ws://127.0.0.1:17419",
        [this](wss::server& server) { server.add_tls_listener(17419, path("server.crt"), path("server.key")); },
        [](wss::client& /*c*/) { /* no enable_tls: plaintext client */ });

    EXPECT_TRUE(result.connect_failed);
    EXPECT_FALSE(result.got_data);
}

// A wss:// client must not succeed against a plaintext listener
TEST_F(TlsTest, TlsClientRejectedByPlaintextListener)
{
    auto result = run_exchange_url(
        "wss://127.0.0.1:17420",
        [](wss::server& server) { server.add_listener(17420); },
        [&](wss::client& c) { c.enable_tls(path("ca.crt")); });

    EXPECT_TRUE(result.connect_failed);
    EXPECT_FALSE(result.got_data);
}

// A single server exposing both a plaintext and a TLS listener serves each client type
TEST_F(TlsTest, MixedPlaintextAndTlsListeners)
{
    const std::uint16_t plain_port = 17421;
    const std::uint16_t tls_port = 17422;

    boost::asio::io_context ioc{1};

    wss::local_signal time_signal{"/Time",
                                  wss::metadata_builder{"Time"}
                                      .data_type(wss::data_types::int64_t)
                                      .unit(wss::unit::seconds)
                                      .linear_rule(0, 1)
                                      .origin(wss::metadata::unix_epoch)
                                      .table("/Time")
                                      .build()};

    wss::local_signal value_signal{"/Channel1/Value",
                                   wss::metadata_builder{"Value"}
                                       .data_type(wss::data_types::int64_t)
                                       .unit(wss::unit::volts)
                                       .range(-100, 100)
                                       .table(time_signal.id())
                                       .build()};

    wss::server server{ioc.get_executor()};
    server.add_listener(plain_port);
    server.add_tls_listener(tls_port, path("server.crt"), path("server.key"));
    server.add_local_signal(time_signal);
    server.add_local_signal(value_signal);
    server.run();

    const std::vector<int64_t> samples{-1, 0, 1, 5, 9, 78, 7, 2, -55, 20};

    exchange_result plain_result;
    exchange_result tls_result;

    std::promise<void> done;
    auto done_future = done.get_future();
    std::atomic<int> remaining{2};
    auto all_done = [&]()
    {
        if (--remaining == 0)
            done.set_value();
    };
    std::atomic<bool> plain_done{false};
    std::atomic<bool> tls_done{false};
    auto finish_plain = [&]()
    {
        if (!plain_done.exchange(true))
            all_done();
    };
    auto finish_tls = [&]()
    {
        if (!tls_done.exchange(true))
            all_done();
    };

    std::vector<boost::signals2::scoped_connection> plain_conns;
    std::vector<boost::signals2::scoped_connection> tls_conns;
    wss::connection_ptr plain_held;
    wss::connection_ptr tls_held;

    wss::client plain_client{ioc.get_executor()};
    wss::client tls_client{ioc.get_executor()};
    tls_client.enable_tls(path("ca.crt"));

    start_subscription(plain_client,
                       "ws://127.0.0.1:" + std::to_string(plain_port),
                       samples,
                       plain_result,
                       finish_plain,
                       plain_conns,
                       plain_held);
    start_subscription(tls_client, "wss://127.0.0.1:" + std::to_string(tls_port), samples, tls_result, finish_tls, tls_conns, tls_held);

    std::thread io_thread{[&] { ioc.run(); }};

    std::atomic<bool> stop{false};
    std::thread publisher{[&]()
                          {
                              std::int64_t t = 0;
                              while (!stop)
                              {
                                  value_signal.publish_data(t++, samples.size(), samples.data(), samples.size() * sizeof(int64_t));
                                  std::this_thread::sleep_for(20ms);
                              }
                          }};

    done_future.wait_for(5s);

    stop = true;
    publisher.join();
    ioc.stop();
    io_thread.join();

    EXPECT_TRUE(plain_result.got_data);
    EXPECT_FALSE(plain_result.connect_failed);
    EXPECT_TRUE(tls_result.got_data);
    EXPECT_FALSE(tls_result.connect_failed);
}

// Two-level chain (root -> intermediate -> leaf)
TEST_F(TlsTest, ServerCertificateChainExchange)
{
    auto result = run_exchange_url(
        "wss://127.0.0.1:17424",
        [this](wss::server& server)
        {
            server.add_tls_listener(17424, path("chain-server.crt"), path("chain-server.key"));
        },
        [&](wss::client& c) { c.enable_tls(path("chain-ca.crt")); });

    EXPECT_TRUE(result.handler_called);
    EXPECT_FALSE(result.connect_failed);
    EXPECT_TRUE(result.got_data);
}

TEST_F(TlsTest, ClientContextRequiresCaFile)
{
    EXPECT_THROW(wss::detail::tls::make_client_tls_context(""), std::invalid_argument);
}

TEST_F(TlsTest, ClientContextRejectsCertWithoutKey)
{
    EXPECT_THROW(wss::detail::tls::make_client_tls_context(path("ca.crt"), path("client.crt"), ""), std::invalid_argument);
    EXPECT_THROW(wss::detail::tls::make_client_tls_context(path("ca.crt"), "", path("client.key")), std::invalid_argument);
}

TEST_F(TlsTest, ClientContextThrowsOnMissingCaFile)
{
    EXPECT_THROW(wss::detail::tls::make_client_tls_context(path("does-not-exist.crt")), boost::system::system_error);
}

TEST_F(TlsTest, ServerContextThrowsOnMissingCertFiles)
{
    EXPECT_THROW(wss::detail::tls::make_server_tls_context(path("does-not-exist.crt"), path("does-not-exist.key")),
                 boost::system::system_error);
}

TEST_F(TlsTest, EnableTlsThrowsOnMissingCaFile)
{
    boost::asio::io_context ioc{1};
    wss::client client{ioc.get_executor()};
    EXPECT_THROW(client.enable_tls(path("does-not-exist.crt")), boost::system::system_error);
}

TEST_F(TlsTest, AddTlsListenerThrowsOnMissingCertFiles)
{
    boost::asio::io_context ioc{1};
    wss::server server{ioc.get_executor()};
    EXPECT_THROW(server.add_tls_listener(17423, path("does-not-exist.crt"), path("does-not-exist.key")), boost::system::system_error);
}

} // namespace
