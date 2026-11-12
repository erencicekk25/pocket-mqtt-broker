#pragma once

#include "Transport.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace pocketmqtt {

#ifdef _WIN32
using NativeSocket = std::uintptr_t;  // Same width as Winsock's SOCKET.
inline constexpr NativeSocket kInvalidSocket = ~static_cast<NativeSocket>(0);
#else
using NativeSocket = int;
inline constexpr NativeSocket kInvalidSocket = -1;
#endif

/// RAII owner of a native socket handle. Move-only; closes the handle on destruction.
class Socket {
public:
    Socket() noexcept = default;
    explicit Socket(NativeSocket handle) noexcept : handle_(handle) {}
    ~Socket() { reset(); }

    Socket(Socket&& other) noexcept : handle_(other.release()) {}
    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            reset(other.release());
        }
        return *this;
    }

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    [[nodiscard]] bool valid() const noexcept { return handle_ != kInvalidSocket; }
    [[nodiscard]] NativeSocket native() const noexcept { return handle_; }

    NativeSocket release() noexcept {
        const NativeSocket handle = handle_;
        handle_ = kInvalidSocket;
        return handle;
    }

    void reset(NativeSocket handle = kInvalidSocket) noexcept;

private:
    NativeSocket handle_ = kInvalidSocket;
};

/// RAII guard for process-wide socket setup: WSAStartup/WSACleanup on Windows,
/// ignoring SIGPIPE on POSIX so a vanished peer cannot kill the process.
class NetworkRuntime {
public:
    NetworkRuntime();
    ~NetworkRuntime();

    NetworkRuntime(const NetworkRuntime&) = delete;
    NetworkRuntime& operator=(const NetworkRuntime&) = delete;
};

struct TcpServerConfig {
    std::string bindAddress = "0.0.0.0";
    std::uint16_t port = 1883;
    int backlog = 512;
    std::size_t maxConnections = 10'000;
    /// A client whose unsent data grows beyond this is considered a slow consumer and dropped.
    std::size_t maxOutboundBufferBytes = 16 * 1024 * 1024;
    /// Upper bound on how long the loop sleeps; also the resolution of `onTick`.
    std::chrono::milliseconds pollInterval{100};
};

/// Single-threaded, non-blocking TCP server built on poll()/WSAPoll().
///
/// All handler callbacks run on the thread that calls `run()`. `send()` and `close()` only
/// touch in-memory buffers and flags, so they are safe to call from inside callbacks.
class TcpServer final : public ITransport {
public:
    explicit TcpServer(TcpServerConfig config);
    ~TcpServer() override;

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;
    TcpServer(TcpServer&&) = delete;
    TcpServer& operator=(TcpServer&&) = delete;

    /// Creates, binds and starts listening on the configured address. Throws on failure.
    void listen();

    /// Runs the event loop until `stop()` is called. All open connections are closed on exit.
    void run(IConnectionHandler& handler);

    /// Asks the event loop to exit. Async-signal-safe (a single lock-free atomic store).
    void stop() noexcept { stopRequested_.store(true, std::memory_order_relaxed); }

    /// The port actually bound; useful when the configured port is 0 (ephemeral).
    [[nodiscard]] std::uint16_t port() const noexcept { return boundPort_; }
    [[nodiscard]] std::size_t connectionCount() const noexcept { return connections_.size(); }

    bool send(ConnectionId id, const std::uint8_t* data, std::size_t size) override;
    void close(ConnectionId id) override;

private:
    struct Connection {
        Socket socket;
        std::string peer;
        std::vector<std::uint8_t> outbox;
        std::size_t outboxOffset = 0;
        bool closing = false;  // Graceful close requested: flush, then close.
        bool dead = false;     // Unusable: close immediately.
        Clock::time_point closeDeadline{};

        [[nodiscard]] std::size_t pendingBytes() const noexcept { return outbox.size() - outboxOffset; }
    };

    void acceptPending(IConnectionHandler& handler);
    void readFrom(ConnectionId id, Connection& connection, IConnectionHandler& handler);
    void flush(Connection& connection);
    void reap(IConnectionHandler& handler);

    NetworkRuntime runtime_;  // Declared first: initialized before and destroyed after every socket.
    TcpServerConfig config_;
    Socket listener_;
    std::uint16_t boundPort_ = 0;
    std::atomic<bool> stopRequested_{false};
    ConnectionId nextConnectionId_ = 1;
    std::unordered_map<ConnectionId, Connection> connections_;
    std::vector<std::uint8_t> readBuffer_;

    static_assert(std::atomic<bool>::is_always_lock_free, "stop() must be async-signal-safe");
};

}  // namespace pocketmqtt
