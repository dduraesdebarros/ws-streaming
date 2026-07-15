#pragma once

#include <string>

#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/verify_mode.hpp>

namespace wss::detail
{
    /**
     * TLS helpers that build a boost::asio::ssl::context from certificate, key, and CA file paths.
     *
     * These implement the library-owned SSL context policy: the application supplies file paths
     * and the library constructs and configures the context with defaults (modern TLS
     * versions only, peer verification enabled).
     *
     * On failure (missing or malformed file), the underlying Boost.Asio calls throw
     * boost::system::system_error.
     */
    namespace tls
    {
        // Disable the obsolete SSL/TLS protocol versions, leaving TLS 1.2 and later
        inline void apply_common_options(boost::asio::ssl::context& ctx)
        {
            ctx.set_options(
                boost::asio::ssl::context::default_workarounds
                    | boost::asio::ssl::context::no_sslv2
                    | boost::asio::ssl::context::no_sslv3
                    | boost::asio::ssl::context::no_tlsv1
                    | boost::asio::ssl::context::no_tlsv1_1);
        }

        /**
         * Builds a client-side TLS context.
         *
         * @param ca_file Path to a PEM file of trusted CA certificates used to verify the server.
         *     Required: if empty, std::invalid_argument is thrown. Peer verification is always enabled.
         * @param cert_file Path to the client certificate chain (PEM), for mutual TLS. Optional,
         *     but if set key_file must also be set.
         * @param key_file Path to the client private key (PEM), for mutual TLS. Optional, but if
         *     set cert_file must also be set.
         *
         * @throws std::invalid_argument ca_file is empty, or exactly one of cert_file and key_file
         *     is set.
         */
        inline boost::asio::ssl::context make_client_tls_context(
            const std::string& ca_file,
            const std::string& cert_file = {},
            const std::string& key_file = {})
        {
            if (cert_file.empty() != key_file.empty())
            {
                throw std::invalid_argument("make_client_tls_context: cert_file and key_file must both be set "
                                            "(mutual TLS) or both be empty (server-only authentication)");
            }

            boost::asio::ssl::context ctx(boost::asio::ssl::context::tls_client);
            apply_common_options(ctx);

            if (ca_file.empty())
                throw std::invalid_argument("make_client_tls_context: ca_file is required");

            ctx.load_verify_file(ca_file);
            ctx.set_verify_mode(boost::asio::ssl::verify_peer);

            if (!cert_file.empty())
                ctx.use_certificate_chain_file(cert_file);

            if (!key_file.empty())
                ctx.use_private_key_file(key_file, boost::asio::ssl::context::pem);

            return ctx;
        }

        /**
         * Builds a server-side TLS context.
         *
         * @param cert_file Path to the server certificate chain (PEM). Required.
         * @param key_file Path to the server private key (PEM). Required.
         * @param ca_file Path to a PEM file of trusted CA certificates used to verify client
         *     certificates. If non-empty, mutual TLS is enabled: clients must present a certificate
         *     signed by one of these CAs. If empty, client certificates are not requested.
         */
        inline boost::asio::ssl::context make_server_tls_context(
            const std::string& cert_file,
            const std::string& key_file,
            const std::string& ca_file = {})
        {
            boost::asio::ssl::context ctx(boost::asio::ssl::context::tls_server);
            apply_common_options(ctx);

            ctx.use_certificate_chain_file(cert_file);
            ctx.use_private_key_file(key_file, boost::asio::ssl::context::pem);

            if (!ca_file.empty())
            {
                ctx.load_verify_file(ca_file);
                ctx.set_verify_mode(boost::asio::ssl::verify_peer | boost::asio::ssl::verify_fail_if_no_peer_cert);
            }

            return ctx;
        }
    }
}
