#include "TcpServer.hpp"

#include "Logger.hpp"

#include <algorithm>
#include <climits>
#include <stdexcept>
#include <system_error>
#include <utility>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace pocketmqtt {
namespace {

constexpr std::size_t kReadChunkBytes = 64 * 1024;
// Fairness: one chatty client may not monopolize a loop iteration.
constexpr std::size_t kReadBudgetPerIteration = 1024 * 1024;
constexpr std::chrono::seconds kGracefulCloseTimeout{2};

#ifdef _WIN32
using PollFd = WSAPOLLFD;
using SockLen = int;
using IoSize = int;

int pollSockets(PollFd* fds, std::size_t count, int timeoutMs) {
    return WSAPoll(fds, static_cast<ULONG>(count), timeoutMs);
}
int lastSocketError() noexcept { return WSAGetLastError(); }
bool wouldBlock(int error) noexcept { return error == WSAEWOULDBLOCK; }
bool interrupted(int error) noexcept { return error == WSAEINTR; }
void closeNative(NativeSocket handle) noexcept { ::closesocket(static_cast<SOCKET>(handle)); }
bool setNonBlocking(NativeSocket handle) noexcept {
    u_long enabled = 1;
    return ::ioctlsocket(static_cast<SOCKET>(handle), FIONBIO, &enabled) == 0;
}
constexpr int kSendFlags = 0;
#else
using PollFd = pollfd;
using SockLen = socklen_t;
using IoSize = std::size_t;

int pollSockets(PollFd* fds, std::size_t count, int timeoutMs) {
    return ::poll(fds, static_cast<nfds_t>(count), timeoutMs);
}
int lastSocketError() noexcept { return errno; }
bool wouldBlock(int error) noexcept { return error == EAGAIN || error == EWOULDBLOCK; }
bool interrupted(int error) noexcept { return error == EINTR; }
void closeNative(NativeSocket handle) noexcept { ::close(handle); }
bool setNonBlocking(NativeSocket handle) noexcept {
    const int flags = ::fcntl(handle, F_GETFL, 0);
    return flags >= 0 && ::fcntl(handle, F_SETFL, flags | O_NONBLOCK) == 0;
}
#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif
#endif

std::string socketErrorMessage(int error) { return std::system_category().message(error); }

[[noreturn]] void throwSocketError(const char* what) {
    throw std::system_error(lastSocketError(), std::system_category(), what);
}

PollFd makePollFd(NativeSocket handle, short events) noexcept {
    PollFd fd{};
    fd.fd = handle;
    fd.events = events;
    return fd;
}

IoSize clampIoSize(std::size_t size) noexcept {
#ifdef _WIN32
    return static_cast<IoSize>(std::min<std::size_t>(size, INT_MAX));
#else
    return size;
#endif
}

std::string formatPeer(const sockaddr_in& address) {
    char host[INET_ADDRSTRLEN] = {};
    if (::inet_ntop(AF_INET, &address.sin_addr, host, sizeof(host)) == nullptr) {
        return "unknown";
    }
    return std::string(host) + ':' + std::to_string(ntohs(address.sin_port));
}

void setOption(NativeSocket handle, int level, int option, int value) noexcept {
#ifdef _WIN32
    ::setsockopt(static_cast<SOCKET>(handle), level, option, reinterpret_cast<const char*>(&value),
                 sizeof(value));
#else
    ::setsockopt(handle, level, option, &value, sizeof(value));
#endif
}

}  // namespace

// ---------------------------------------------------------------------------------------------
// Socket / NetworkRuntime
// ---------------------------------------------------------------------------------------------

void Socket::reset(NativeSocket handle) noexcept {
    if (handle_ != kInvalidSocket) {
        closeNative(handle_);
    }
    handle_ = handle;
}

NetworkRuntime::NetworkRuntime() {
#ifdef _WIN32
    WSADATA data{};
    const int result = ::WSAStartup(MAKEWORD(2, 2), &data);
    if (result != 0) {
        throw std::system_error(result, std::system_category(), "WSAStartup");
    }
#else
    std::signal(SIGPIPE, SIG_IGN);
#endif
}

NetworkRuntime::~NetworkRuntime() {
#ifdef _WIN32
    ::WSACleanup();
#endif
}

// ---------------------------------------------------------------------------------------------
// TcpServer
// ---------------------------------------------------------------------------------------------

TcpServer::TcpServer(TcpServerConfig config)
    : config_(std::move(config)), readBuffer_(kReadChunkBytes) {}

TcpServer::~TcpServer() = default;

void TcpServer::listen() {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(config_.port);
    if (::inet_pton(AF_INET, config_.bindAddress.c_str(), &address.sin_addr) != 1) {
        throw std::invalid_argument("invalid IPv4 bind address: " + config_.bindAddress);
    }

    Socket socket(static_cast<NativeSocket>(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)));
    if (!socket.valid()) {
        throwSocketError("socket");
    }

#ifdef _WIN32
    // On Windows SO_REUSEADDR allows port hijacking; exclusive use is the safe equivalent.
    setOption(socket.native(), SOL_SOCKET, SO_EXCLUSIVEADDRUSE, 1);
    const SOCKET native = static_cast<SOCKET>(socket.native());
#else
    setOption(socket.native(), SOL_SOCKET, SO_REUSEADDR, 1);
    const int native = socket.native();
#endif

    if (::bind(native, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        throwSocketError("bind");
    }
    if (::listen(native, config_.backlog) != 0) {
        throwSocketError("listen");
    }
    if (!setNonBlocking(socket.native())) {
        throwSocketError("set non-blocking");
    }

    sockaddr_in bound{};
    SockLen length = sizeof(bound);
    if (::getsockname(native, reinterpret_cast<sockaddr*>(&bound), &length) != 0) {
        throwSocketError("getsockname");
    }
    boundPort_ = ntohs(bound.sin_port);
    listener_ = std::move(socket);
}

void TcpServer::run(IConnectionHandler& handler) {
    if (!listener_.valid()) {
        throw std::logic_error("TcpServer::run() called before listen()");
    }

    std::vector<PollFd> fds;
    std::vector<ConnectionId> ids;  // ids[i] belongs to fds[i + 1]; fds[0] is the listener.
    const int timeoutMs = static_cast<int>(config_.pollInterval.count());

    while (!stopRequested_.load(std::memory_order_relaxed)) {
        fds.clear();
        ids.clear();
        fds.push_back(makePollFd(listener_.native(), POLLIN));
        for (const auto& [id, connection] : connections_) {
            if (connection.dead) {
                continue;
            }
            short events = 0;
            if (!connection.closing) events |= POLLIN;
            if (connection.pendingBytes() > 0) events |= POLLOUT;
            if (events != 0) {
                fds.push_back(makePollFd(connection.socket.native(), events));
                ids.push_back(id);
            }
        }

        const int ready = pollSockets(fds.data(), fds.size(), timeoutMs);
        if (ready < 0) {
            const int error = lastSocketError();
            if (interrupted(error)) {
                continue;
            }
            throw std::system_error(error, std::system_category(), "poll");
        }

        if (ready > 0) {
            if ((fds[0].revents & POLLIN) != 0) {
                acceptPending(handler);
            }
            for (std::size_t i = 1; i < fds.size(); ++i) {
                const short revents = fds[i].revents;
                if (revents == 0) {
                    continue;
                }
                const auto it = connections_.find(ids[i - 1]);
                if (it == connections_.end()) {
                    continue;
                }
                Connection& connection = it->second;
                if ((revents & POLLNVAL) != 0) {
                    connection.dead = true;
                } else if ((revents & (POLLIN | POLLHUP | POLLERR)) != 0) {
                    if (!connection.closing && !connection.dead) {
                        readFrom(it->first, connection, handler);
                    } else if ((revents & (POLLHUP | POLLERR)) != 0) {
                        connection.dead = true;
                    }
                }
            }
        }

        // Flush eagerly: data produced while handling this iteration goes out right away
        // instead of waiting for the next POLLOUT round trip.
        for (auto& entry : connections_) {
            if (!entry.second.dead && entry.second.pendingBytes() > 0) {
                flush(entry.second);
            }
        }

        handler.onTick(Clock::now());
        reap(handler);
    }

    // Shutdown: give queued data one last chance, then close everything.
    for (auto& entry : connections_) {
        if (!entry.second.dead) {
            flush(entry.second);
        }
        entry.second.dead = true;
    }
    reap(handler);
}

bool TcpServer::send(ConnectionId id, const std::uint8_t* data, std::size_t size) {
    const auto it = connections_.find(id);
    if (it == connections_.end() || it->second.dead || it->second.closing) {
        return false;
    }
    Connection& connection = it->second;
    if (connection.pendingBytes() + size > config_.maxOutboundBufferBytes) {
        logWarn("tcp", "dropping slow consumer ", connection.peer, " (", connection.pendingBytes(),
                " bytes unsent)");
        connection.dead = true;
        return false;
    }
    connection.outbox.insert(connection.outbox.end(), data, data + size);
    return true;
}

void TcpServer::close(ConnectionId id) {
    const auto it = connections_.find(id);
    if (it == connections_.end() || it->second.dead || it->second.closing) {
        return;
    }
    it->second.closing = true;
    it->second.closeDeadline = Clock::now() + kGracefulCloseTimeout;
}

void TcpServer::acceptPending(IConnectionHandler& handler) {
    for (;;) {
        sockaddr_in address{};
        SockLen length = sizeof(address);
#ifdef _WIN32
        const SOCKET listener = static_cast<SOCKET>(listener_.native());
#else
        const int listener = listener_.native();
#endif
        Socket client(static_cast<NativeSocket>(
            ::accept(listener, reinterpret_cast<sockaddr*>(&address), &length)));
        if (!client.valid()) {
            const int error = lastSocketError();
            if (!wouldBlock(error) && !interrupted(error)) {
                logWarn("tcp", "accept failed: ", socketErrorMessage(error));
            }
            return;
        }

        const std::string peer = formatPeer(address);
        if (connections_.size() >= config_.maxConnections) {
            logWarn("tcp", "connection limit (", config_.maxConnections, ") reached, rejecting ", peer);
            continue;  // `client` closes the socket on scope exit.
        }
        if (!setNonBlocking(client.native())) {
            logWarn("tcp", "could not make socket non-blocking for ", peer);
            continue;
        }
        // MQTT traffic is many small packets; Nagle's algorithm only adds latency here.
        setOption(client.native(), IPPROTO_TCP, TCP_NODELAY, 1);

        const ConnectionId id = nextConnectionId_++;
        Connection connection;
        connection.socket = std::move(client);
        connection.peer = peer;
        connections_.emplace(id, std::move(connection));
        logDebug("tcp", "accepted #", id, " from ", peer);
        handler.onAccept(id, peer);
    }
}

void TcpServer::readFrom(ConnectionId id, Connection& connection, IConnectionHandler& handler) {
#ifdef _WIN32
    const SOCKET native = static_cast<SOCKET>(connection.socket.native());
#else
    const int native = connection.socket.native();
#endif
    std::size_t budget = kReadBudgetPerIteration;
    while (budget > 0 && !connection.closing && !connection.dead) {
        const auto received = ::recv(native, reinterpret_cast<char*>(readBuffer_.data()),
                                     clampIoSize(readBuffer_.size()), 0);
        if (received > 0) {
            const auto count = static_cast<std::size_t>(received);
            budget -= std::min(budget, count);
            handler.onData(id, readBuffer_.data(), count);
            continue;
        }
        if (received == 0) {
            logDebug("tcp", "peer ", connection.peer, " closed the connection");
            connection.dead = true;
            return;
        }
        const int error = lastSocketError();
        if (wouldBlock(error)) {
            return;
        }
        if (interrupted(error)) {
            continue;
        }
        logDebug("tcp", "recv from ", connection.peer, " failed: ", socketErrorMessage(error));
        connection.dead = true;
        return;
    }
}

void TcpServer::flush(Connection& connection) {
#ifdef _WIN32
    const SOCKET native = static_cast<SOCKET>(connection.socket.native());
#else
    const int native = connection.socket.native();
#endif
    while (connection.pendingBytes() > 0) {
        const auto* data =
            reinterpret_cast<const char*>(connection.outbox.data() + connection.outboxOffset);
        const auto sent = ::send(native, data, clampIoSize(connection.pendingBytes()), kSendFlags);
        if (sent > 0) {
            connection.outboxOffset += static_cast<std::size_t>(sent);
            continue;
        }
        const int error = lastSocketError();
        if (sent < 0 && wouldBlock(error)) {
            break;
        }
        if (sent < 0 && interrupted(error)) {
            continue;
        }
        logDebug("tcp", "send to ", connection.peer, " failed: ", socketErrorMessage(error));
        connection.dead = true;
        return;
    }

    if (connection.pendingBytes() == 0) {
        connection.outbox.clear();
        connection.outboxOffset = 0;
    } else if (connection.outboxOffset > connection.outbox.size() / 2) {
        // Compact occasionally so a long-lived slow reader does not pin consumed bytes.
        connection.outbox.erase(connection.outbox.begin(),
                                connection.outbox.begin() +
                                    static_cast<std::ptrdiff_t>(connection.outboxOffset));
        connection.outboxOffset = 0;
    }
}

void TcpServer::reap(IConnectionHandler& handler) {
    const auto now = Clock::now();
    std::vector<ConnectionId> closed;
    for (auto it = connections_.begin(); it != connections_.end();) {
        const Connection& connection = it->second;
        const bool drained = connection.pendingBytes() == 0;
        if (connection.dead ||
            (connection.closing && (drained || now >= connection.closeDeadline))) {
            logDebug("tcp", "closed #", it->first, " (", connection.peer, ")");
            closed.push_back(it->first);
            it = connections_.erase(it);  // Socket destructor closes the handle.
        } else {
            ++it;
        }
    }
    // Notify only after erasing, so handlers that send to these ids are safely ignored.
    for (const ConnectionId id : closed) {
        handler.onClose(id);
    }
}

}  // namespace pocketmqtt
