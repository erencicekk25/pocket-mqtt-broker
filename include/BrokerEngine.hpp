#pragma once

#include "MqttProtocol.hpp"
#include "TopicTree.hpp"
#include "Transport.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace pocketmqtt {

struct BrokerConfig {
    /// Largest accepted packet body; bigger packets close the connection.
    std::size_t maxPacketSize = 1024 * 1024;
    /// QoS 1 messages buffered per session while offline or while the in-flight window is full.
    std::size_t maxQueuedMessages = 1000;
    /// Unacknowledged outgoing QoS 1 messages allowed per session.
    std::size_t maxInflightMessages = 32;
    /// A connection must send CONNECT within this time.
    std::chrono::seconds connectTimeout{10};
    /// Interval for periodic statistics log lines; zero disables them.
    std::chrono::seconds statsInterval{0};
};

struct BrokerStats {
    std::uint64_t connectionsAccepted = 0;
    std::uint64_t messagesReceived = 0;
    std::uint64_t messagesDelivered = 0;
    std::uint64_t messagesDropped = 0;
    std::size_t clientsConnected = 0;
    std::size_t sessions = 0;
    std::size_t subscriptions = 0;
    std::size_t retainedMessages = 0;
};

/// The MQTT 3.1.1 broker: sessions, subscriptions, retained messages, wills and routing.
///
/// The engine is transport-agnostic: it consumes bytes through `IConnectionHandler` and
/// produces bytes through `ITransport`. It is single-threaded by design and must be driven
/// from one thread (the TcpServer event loop), which removes every lock from the hot path.
class BrokerEngine final : public IConnectionHandler {
public:
    explicit BrokerEngine(ITransport& transport, BrokerConfig config = {});
    ~BrokerEngine() override;

    BrokerEngine(const BrokerEngine&) = delete;
    BrokerEngine& operator=(const BrokerEngine&) = delete;
    BrokerEngine(BrokerEngine&&) = delete;
    BrokerEngine& operator=(BrokerEngine&&) = delete;

    void onAccept(ConnectionId id, const std::string& peer) override;
    void onData(ConnectionId id, const std::uint8_t* data, std::size_t size) override;
    void onClose(ConnectionId id) override;
    void onTick(Clock::time_point now) override;

    [[nodiscard]] BrokerStats stats() const;

private:
    using SessionId = TopicTree::SubscriberId;

    /// An application message, shared (never copied) between all sessions it is queued for.
    struct Message {
        std::string topic;
        std::vector<std::uint8_t> payload;
        mqtt::QoS qos = mqtt::QoS::AtMostOnce;
        bool retain = false;
    };
    using MessagePtr = std::shared_ptr<const Message>;

    struct PendingDelivery {
        MessagePtr message;
        mqtt::QoS qos = mqtt::QoS::AtMostOnce;
        bool retain = false;  ///< True only when delivering a stored retained message.
    };

    struct Session {
        SessionId id = 0;
        std::string clientId;
        bool cleanSession = true;
        std::optional<ConnectionId> connection;  ///< Empty while a persistent session is offline.
        std::map<std::string, mqtt::QoS> subscriptions;
        std::deque<PendingDelivery> queue;              ///< Waiting for an in-flight slot.
        std::map<std::uint16_t, PendingDelivery> inflight;  ///< Sent, awaiting PUBACK.
        std::unordered_set<std::uint16_t> awaitingRelease;  ///< Inbound QoS 2, awaiting PUBREL.
        std::uint16_t nextPacketId = 1;
        bool queueOverflowReported = false;
    };

    struct ClientConnection {
        ClientConnection(ConnectionId connectionId, std::string peerAddress, std::size_t maxPacketSize,
                         Clock::time_point now)
            : id(connectionId),
              peer(std::move(peerAddress)),
              decoder(mqtt::Role::Server, maxPacketSize),
              acceptedAt(now),
              lastActivity(now) {}

        ConnectionId id = 0;
        std::string peer;
        mqtt::FrameDecoder decoder;
        std::optional<SessionId> session;  ///< Set once CONNECT has been accepted.
        std::string clientId;
        std::uint16_t keepAliveSeconds = 0;
        std::optional<mqtt::Will> will;
        Clock::time_point acceptedAt{};
        Clock::time_point lastActivity{};
        bool closing = false;
        const char* closeReason = "connection lost";
    };

    class PublishEncoder;

    // Packet handlers. Each returns false when the connection was closed as a result.
    bool handlePacket(ClientConnection& client, mqtt::Packet& packet);
    bool handleConnect(ClientConnection& client, mqtt::ConnectPacket& packet);
    bool handlePublish(ClientConnection& client, mqtt::PublishPacket& packet);
    bool handleAck(ClientConnection& client, const mqtt::AckPacket& packet);
    bool handleSubscribe(ClientConnection& client, const mqtt::SubscribePacket& packet);
    bool handleUnsubscribe(ClientConnection& client, const mqtt::UnsubscribePacket& packet);

    // Routing and delivery.
    void publish(MessagePtr message);
    void deliver(Session& session, const MessagePtr& message, mqtt::QoS qos, PublishEncoder& encoder);
    void enqueue(Session& session, PendingDelivery delivery);
    void drainQueue(Session& session);
    void resendInflight(Session& session);
    std::uint16_t allocatePacketId(Session& session);

    // Session lifecycle.
    Session& createSession(const std::string& clientId, bool cleanSession);
    void destroySession(SessionId id);
    std::string generateClientId();

    void sendPacket(ConnectionId id, const std::vector<std::uint8_t>& bytes);
    void closeConnection(ClientConnection& client, const char* reason);
    void logStats();
    [[nodiscard]] static std::string describe(const ClientConnection& client);

    ITransport& transport_;
    BrokerConfig config_;

    std::unordered_map<ConnectionId, ClientConnection> connections_;
    std::unordered_map<SessionId, Session> sessions_;
    std::unordered_map<std::string, SessionId> sessionsByClientId_;
    TopicTree subscriptions_;
    std::map<std::string, MessagePtr, std::less<>> retained_;

    SessionId nextSessionId_ = 1;
    std::uint64_t generatedClientIds_ = 0;
    BrokerStats counters_;
    TopicTree::MatchResult matchScratch_;  ///< Reused across publishes to avoid reallocations.
    Clock::time_point lastStatsLog_ = Clock::now();
};

}  // namespace pocketmqtt
