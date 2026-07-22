find_package(OpenSSL 1.1.1 QUIET GLOBAL)

if(OpenSSL_FOUND)

    message(STATUS "Found OpenSSL ${OPENSSL_VERSION} at ${OPENSSL_INCLUDE_DIR}")

else()

    message(FATAL_ERROR
        "OpenSSL >= 1.1.1 was not found. It is required for TLS (wss://) support, and, unlike the"
        " other dependencies, it is not fetched automatically: it must be installed on the host"
        " system first. It is packaged as libssl-dev (Debian, Ubuntu), openssl-devel (Fedora,"
        " RHEL), openssl (Homebrew) or openssl (vcpkg).")

endif()
