#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

namespace pocketmqtt {

/// Opaque, never-reused identifier of a single client connection.
using ConnectionId = std::uint64_t;
using Clock = std::chrono::steady_clock;

/// Outbound side of the network layer. The broker engine talks to the network only
/// through this interface, which keeps it independent from sockets and easy to test.
class ITransport {
public:
    virtual ~ITransport() = default;

    /// Queues bytes for delivery. Returns false when the connection is unknown,
    /// closing, or has been dropped (for example because it is a slow consumer).
    virtual bool send(ConnectionId id, const std::uint8_t* data, std::size_t size) = 0;

    /// Requests a graceful close: queued bytes are flushed first, then the socket is closed
    /// and `IConnectionHandler::onClose` is invoked.
    virtual void close(ConnectionId id) = 0;

protected:
    ITransport() = default;
    ITransport(const ITransport&) = default;
    ITransport& operator=(const ITransport&) = default;
};

/// Inbound side of the network layer: receives connection lifecycle events and data.
///
/// Contract: for every `onAccept(id)` there is exactly one later `onClose(id)`,
/// and no `onData(id)` is delivered after it.
class IConnectionHandler {
public:
    virtual ~IConnectionHandler() = default;

    virtual void onAccept(ConnectionId id, const std::string& peer) = 0;
    virtual void onData(ConnectionId id, const std::uint8_t* data, std::size_t size) = 0;
    virtual void onClose(ConnectionId id) = 0;

    /// Called once per event-loop iteration; used for timers such as keep-alive checks.
    virtual void onTick(Clock::time_point now) = 0;

protected:
    IConnectionHandler() = default;
    IConnectionHandler(const IConnectionHandler&) = default;
    IConnectionHandler& operator=(const IConnectionHandler&) = default;
};

}  // namespace pocketmqtt
