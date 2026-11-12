#include "BrokerEngine.hpp"

#include "Logger.hpp"

#include <algorithm>
#include <type_traits>
#include <utility>
#include <variant>

namespace pocketmqtt {
namespace {

using mqtt::QoS;

template <typename... Handlers>
struct Overloaded : Handlers... {
    using Handlers::operator()...;
};
template <typename... Handlers>
Overloaded(Handlers...) -> Overloaded<Handlers...>;

constexpr std::uint8_t kPingresp[] = {0xD0, 0x00};

int qosNumber(QoS qos) noexcept { return static_cast<int>(qos); }

}  // namespace

/// Encodes one message for many subscribers. Each wire variant is built at most once per
/// publish; for QoS 1 only the two packet-identifier bytes (and the DUP bit) are patched
/// per recipient instead of re-encoding topic and payload every time.
class BrokerEngine::PublishEncoder {
public:
    PublishEncoder(const Message& message, bool retain) noexcept
        : message_(message), retain_(retain) {}

    [[nodiscard]] bool retain() const noexcept { return retain_; }

    const std::vector<std::uint8_t>& atMostOnce() {
        if (atMostOnce_.empty()) {
            atMostOnce_ = mqtt::encodePublish(message_.topic, message_.payload.data(),
                                              message_.payload.size(), QoS::AtMostOnce, retain_,
                                              false, 0);
        }
        return atMostOnce_;
    }

    const std::vector<std::uint8_t>& atLeastOnce(std::uint16_t packetId, bool dup) {
        if (atLeastOnce_.empty()) {
            atLeastOnce_ = mqtt::encodePublish(message_.topic, message_.payload.data(),
                                               message_.payload.size(), QoS::AtLeastOnce, retain_,
                                               false, 1);
            // The packet identifier sits right before the payload.
            packetIdOffset_ = atLeastOnce_.size() - message_.payload.size() - 2;
        }
        atLeastOnce_[0] = static_cast<std::uint8_t>(dup ? (atLeastOnce_[0] | 0x08)
                                                        : (atLeastOnce_[0] & ~0x08));
        atLeastOnce_[packetIdOffset_] = static_cast<std::uint8_t>(packetId >> 8);
        atLeastOnce_[packetIdOffset_ + 1] = static_cast<std::uint8_t>(packetId & 0xFF);
        return atLeastOnce_;
    }

private:
    const Message& message_;
    bool retain_;
    std::vector<std::uint8_t> atMostOnce_;
    std::vector<std::uint8_t> atLeastOnce_;
    std::size_t packetIdOffset_ = 0;
};

BrokerEngine::BrokerEngine(ITransport& transport, BrokerConfig config)
    : transport_(transport), config_(config) {}

BrokerEngine::~BrokerEngine() = default;

// ---------------------------------------------------------------------------------------------
// Connection events
// ---------------------------------------------------------------------------------------------

void BrokerEngine::onAccept(ConnectionId id, const std::string& peer) {
    ++counters_.connectionsAccepted;
    connections_.emplace(id, ClientConnection(id, peer, config_.maxPacketSize, Clock::now()));
}

void BrokerEngine::onData(ConnectionId id, const std::uint8_t* data, std::size_t size) {
    const auto it = connections_.find(id);
    if (it == connections_.end() || it->second.closing) {
        return;
    }
    ClientConnection& client = it->second;
    client.lastActivity = Clock::now();  // Any packet counts as keep-alive activity.
    client.decoder.feed(data, size);

    mqtt::Packet packet;
    while (!client.closing) {
        const mqtt::DecodeStatus status = client.decoder.next(packet);
        if (status == mqtt::DecodeStatus::NeedMoreData) {
            return;
        }
        if (status != mqtt::DecodeStatus::Ok) {
            logWarn("broker", describe(client), " sent an invalid packet: ", mqtt::toString(status));
            closeConnection(client, mqtt::toString(status));
            return;
        }
        if (!handlePacket(client, packet)) {
            return;
        }
    }
}

void BrokerEngine::onClose(ConnectionId id) {
    const auto it = connections_.find(id);
    if (it == connections_.end()) {
        return;
    }
    ClientConnection client = std::move(it->second);
    connections_.erase(it);

    if (client.session) {
        const auto session = sessions_.find(*client.session);
        if (session != sessions_.end() && session->second.connection == id) {
            session->second.connection.reset();
            logInfo("broker", "client '", client.clientId, "' disconnected (", client.closeReason, ")");
            if (session->second.cleanSession) {
                destroySession(session->first);
            }
        }
    }

    // MQTT-3.1.2-8: the will is published whenever the connection ends without DISCONNECT.
    if (client.will) {
        logInfo("broker", "publishing will of '", client.clientId, "' to '", client.will->topic, "'");
        publish(std::make_shared<const Message>(Message{std::move(client.will->topic),
                                                        std::move(client.will->payload),
                                                        client.will->qos, client.will->retain}));
    }
}

void BrokerEngine::onTick(Clock::time_point now) {
    for (auto& entry : connections_) {
        ClientConnection& client = entry.second;
        if (client.closing) {
            continue;
        }
        if (!client.session) {
            if (now - client.acceptedAt > config_.connectTimeout) {
                logWarn("broker", client.peer, " did not send CONNECT in time");
                closeConnection(client, "CONNECT timeout");
            }
            continue;
        }
        // MQTT-3.1.2-24: allow one and a half keep-alive periods of silence.
        const auto grace = std::chrono::milliseconds(client.keepAliveSeconds * 1500LL);
        if (client.keepAliveSeconds > 0 && now - client.lastActivity > grace) {
            logWarn("broker", describe(client), " keep-alive expired");
            closeConnection(client, "keep-alive timeout");
        }
    }

    if (config_.statsInterval.count() > 0 && now - lastStatsLog_ >= config_.statsInterval) {
        lastStatsLog_ = now;
        logStats();
    }
}

// ---------------------------------------------------------------------------------------------
// Packet handlers
// ---------------------------------------------------------------------------------------------

bool BrokerEngine::handlePacket(ClientConnection& client, mqtt::Packet& packet) {
    if (!client.session && !std::holds_alternative<mqtt::ConnectPacket>(packet)) {
        logWarn("broker", client.peer, " sent ", mqtt::toString(mqtt::packetTypeOf(packet)),
                " before CONNECT");
        closeConnection(client, "packet before CONNECT");  // MQTT-3.1.0-1
        return false;
    }

    return std::visit(
        Overloaded{
            [&](mqtt::ConnectPacket& connect) { return handleConnect(client, connect); },
            [&](mqtt::PublishPacket& publishPacket) { return handlePublish(client, publishPacket); },
            [&](const mqtt::AckPacket& ack) { return handleAck(client, ack); },
            [&](const mqtt::SubscribePacket& subscribe) { return handleSubscribe(client, subscribe); },
            [&](const mqtt::UnsubscribePacket& unsubscribe) {
                return handleUnsubscribe(client, unsubscribe);
            },
            [&](const mqtt::PingreqPacket&) {
                transport_.send(client.id, kPingresp, sizeof(kPingresp));
                return true;
            },
            [&](const mqtt::DisconnectPacket&) {
                client.will.reset();  // MQTT-3.14.4-3: a clean disconnect discards the will.
                closeConnection(client, "client sent DISCONNECT");
                return false;
            },
            [&](const auto&) {
                // Server-to-client packets are already rejected by the decoder.
                closeConnection(client, "unexpected packet");
                return false;
            },
        },
        packet);
}

bool BrokerEngine::handleConnect(ClientConnection& client, mqtt::ConnectPacket& packet) {
    if (client.session) {
        logWarn("broker", describe(client), " sent a second CONNECT");
        closeConnection(client, "duplicate CONNECT");  // MQTT-3.1.0-2
        return false;
    }

    const auto reject = [&](mqtt::ConnectReturnCode code) {
        sendPacket(client.id, mqtt::encodeConnack({false, code}));
        logWarn("broker", "rejected client '", packet.clientId, "' from ", client.peer, ": ",
                mqtt::toString(code));
        closeConnection(client, "connection refused");
        return false;
    };

    if (packet.protocolName != mqtt::kProtocolName && packet.protocolName != "MQIsdp") {
        logWarn("broker", client.peer, " is not speaking MQTT (protocol '", packet.protocolName, "')");
        closeConnection(client, "unknown protocol");  // MQTT-3.1.2-1
        return false;
    }
    if (packet.protocolName != mqtt::kProtocolName ||
        packet.protocolLevel != mqtt::kProtocolLevel311) {
        return reject(mqtt::ConnectReturnCode::UnacceptableProtocolVersion);
    }
    if (packet.clientId.empty()) {
        if (!packet.cleanSession) {
            return reject(mqtt::ConnectReturnCode::IdentifierRejected);  // MQTT-3.1.3-8
        }
        packet.clientId = generateClientId();
    }

    bool sessionPresent = false;
    SessionId resumedId = 0;
    if (const auto existing = sessionsByClientId_.find(packet.clientId);
        existing != sessionsByClientId_.end()) {
        Session& previous = sessions_.at(existing->second);
        if (previous.connection) {
            // MQTT-3.1.4-2: a second client with the same id takes over the session.
            const auto old = connections_.find(*previous.connection);
            if (old != connections_.end()) {
                logInfo("broker", "client '", packet.clientId, "' took over from ", old->second.peer);
                old->second.session.reset();
                closeConnection(old->second, "session taken over");
            }
            previous.connection.reset();
        }
        if (packet.cleanSession || previous.cleanSession) {
            destroySession(previous.id);
        } else {
            sessionPresent = true;
            resumedId = previous.id;
        }
    }

    Session& session =
        sessionPresent ? sessions_.at(resumedId) : createSession(packet.clientId, packet.cleanSession);
    session.connection = client.id;
    client.session = session.id;
    client.clientId = session.clientId;
    client.keepAliveSeconds = packet.keepAliveSeconds;
    client.will = std::move(packet.will);

    sendPacket(client.id, mqtt::encodeConnack({sessionPresent, mqtt::ConnectReturnCode::Accepted}));
    logInfo("broker", "client '", client.clientId, "' connected from ", client.peer,
            " (clean session: ", packet.cleanSession ? "yes" : "no",
            ", keep-alive: ", client.keepAliveSeconds, "s",
            sessionPresent ? ", session resumed" : "", ")");

    if (sessionPresent) {
        resendInflight(session);
        drainQueue(session);
    }
    return true;
}

bool BrokerEngine::handlePublish(ClientConnection& client, mqtt::PublishPacket& packet) {
    Session& session = sessions_.at(*client.session);
    const QoS qos = packet.qos;
    const std::uint16_t packetId = packet.packetId;
    ++counters_.messagesReceived;

    // QoS 2 (method B from the spec): route on first receipt, remember the id until PUBREL so
    // a retransmitted PUBLISH is acknowledged again but never routed twice.
    const bool duplicateQos2 =
        qos == QoS::ExactlyOnce && !session.awaitingRelease.insert(packetId).second;

    if (!duplicateQos2) {
        logDebug("broker", "'", client.clientId, "' published ", packet.payload.size(),
                 " bytes to '", packet.topic, "' (QoS ", qosNumber(qos),
                 packet.retain ? ", retained" : "", ")");
        publish(std::make_shared<const Message>(
            Message{std::move(packet.topic), std::move(packet.payload), qos, packet.retain}));
    }

    if (qos == QoS::AtLeastOnce) {
        sendPacket(client.id, mqtt::encodeAck({mqtt::PacketType::Puback, packetId}));
    } else if (qos == QoS::ExactlyOnce) {
        sendPacket(client.id, mqtt::encodeAck({mqtt::PacketType::Pubrec, packetId}));
    }
    return true;
}

bool BrokerEngine::handleAck(ClientConnection& client, const mqtt::AckPacket& packet) {
    Session& session = sessions_.at(*client.session);
    switch (packet.type) {
        case mqtt::PacketType::Puback:
            if (session.inflight.erase(packet.packetId) > 0) {
                drainQueue(session);
            }
            return true;
        case mqtt::PacketType::Pubrel:
            session.awaitingRelease.erase(packet.packetId);
            sendPacket(client.id, mqtt::encodeAck({mqtt::PacketType::Pubcomp, packet.packetId}));
            return true;
        case mqtt::PacketType::Pubrec:
        case mqtt::PacketType::Pubcomp:
            // Outbound delivery is capped at QoS 1, so these never answer anything of ours.
            logDebug("broker", describe(client), " sent unexpected ", mqtt::toString(packet.type));
            return true;
        default:
            closeConnection(client, "unexpected acknowledgement");
            return false;
    }
}

bool BrokerEngine::handleSubscribe(ClientConnection& client, const mqtt::SubscribePacket& packet) {
    Session& session = sessions_.at(*client.session);
    mqtt::SubackPacket ack{packet.packetId, {}};
    ack.returnCodes.reserve(packet.requests.size());
    std::vector<std::pair<const std::string*, QoS>> granted;

    for (const mqtt::SubscriptionRequest& request : packet.requests) {
        if (!isValidTopicFilter(request.filter)) {
            logWarn("broker", describe(client), " sent invalid topic filter '", request.filter, "'");
            ack.returnCodes.push_back(mqtt::kSubackFailure);
            continue;
        }
        const QoS qos = std::min(request.qos, QoS::AtLeastOnce);
        session.subscriptions[request.filter] = qos;
        subscriptions_.subscribe(request.filter, session.id, qos);
        ack.returnCodes.push_back(static_cast<std::uint8_t>(qos));
        granted.emplace_back(&request.filter, qos);
        logInfo("broker", "'", client.clientId, "' subscribed to '", request.filter, "' (QoS ",
                qosNumber(qos), ")");
    }
    sendPacket(client.id, mqtt::encodeSuback(ack));

    // Retained messages follow the SUBACK (MQTT-3.3.1-6).
    for (const auto& [filter, qos] : granted) {
        for (const auto& [topic, message] : retained_) {
            if (topicMatches(*filter, topic)) {
                PublishEncoder encoder(*message, true);
                deliver(session, message, std::min(message->qos, qos), encoder);
            }
        }
    }
    return true;
}

bool BrokerEngine::handleUnsubscribe(ClientConnection& client,
                                     const mqtt::UnsubscribePacket& packet) {
    Session& session = sessions_.at(*client.session);
    for (const std::string& filter : packet.filters) {
        if (session.subscriptions.erase(filter) > 0) {
            subscriptions_.unsubscribe(filter, session.id);
            logInfo("broker", "'", client.clientId, "' unsubscribed from '", filter, "'");
        }
    }
    sendPacket(client.id, mqtt::encodeAck({mqtt::PacketType::Unsuback, packet.packetId}));
    return true;
}

// ---------------------------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------------------------

void BrokerEngine::publish(MessagePtr message) {
    if (message->retain) {
        if (message->payload.empty()) {
            // MQTT-3.3.1-10: an empty retained message clears the retained slot.
            if (const auto it = retained_.find(message->topic); it != retained_.end()) {
                retained_.erase(it);
            }
        } else {
            retained_[message->topic] = message;
        }
    }

    subscriptions_.match(message->topic, matchScratch_);
    if (matchScratch_.empty()) {
        return;
    }
    // MQTT-3.3.1-9: live deliveries always carry RETAIN = 0.
    PublishEncoder encoder(*message, false);
    for (const auto& [sessionId, grantedQos] : matchScratch_) {
        const auto it = sessions_.find(sessionId);
        if (it != sessions_.end()) {
            deliver(it->second, message, std::min(message->qos, grantedQos), encoder);
        }
    }
}

void BrokerEngine::deliver(Session& session, const MessagePtr& message, QoS qos,
                           PublishEncoder& encoder) {
    if (!session.connection) {
        if (qos == QoS::AtMostOnce) {
            ++counters_.messagesDropped;  // QoS 0 is never stored for offline sessions.
        } else {
            enqueue(session, {message, qos, encoder.retain()});
        }
        return;
    }

    if (qos == QoS::AtMostOnce) {
        const std::vector<std::uint8_t>& bytes = encoder.atMostOnce();
        if (transport_.send(*session.connection, bytes.data(), bytes.size())) {
            ++counters_.messagesDelivered;
        } else {
            ++counters_.messagesDropped;
        }
        return;
    }

    if (session.inflight.size() >= config_.maxInflightMessages) {
        enqueue(session, {message, qos, encoder.retain()});
        return;
    }
    const std::uint16_t packetId = allocatePacketId(session);
    session.inflight.emplace(packetId, PendingDelivery{message, qos, encoder.retain()});
    const std::vector<std::uint8_t>& bytes = encoder.atLeastOnce(packetId, false);
    transport_.send(*session.connection, bytes.data(), bytes.size());
    ++counters_.messagesDelivered;
}

void BrokerEngine::enqueue(Session& session, PendingDelivery delivery) {
    if (config_.maxQueuedMessages == 0) {
        ++counters_.messagesDropped;
        return;
    }
    if (session.queue.size() >= config_.maxQueuedMessages) {
        session.queue.pop_front();  // Bounded memory: the oldest message gives way.
        ++counters_.messagesDropped;
        if (!session.queueOverflowReported) {
            session.queueOverflowReported = true;  // Warn once per overflow episode, not per message.
            logWarn("broker", "queue of '", session.clientId, "' is full (", config_.maxQueuedMessages,
                    " messages), dropping oldest messages");
        }
    }
    session.queue.push_back(std::move(delivery));
}

void BrokerEngine::drainQueue(Session& session) {
    while (session.connection && !session.queue.empty() &&
           session.inflight.size() < config_.maxInflightMessages) {
        PendingDelivery next = std::move(session.queue.front());
        session.queue.pop_front();

        PublishEncoder encoder(*next.message, next.retain);
        const std::uint16_t packetId = allocatePacketId(session);
        const std::vector<std::uint8_t>& bytes = encoder.atLeastOnce(packetId, false);
        transport_.send(*session.connection, bytes.data(), bytes.size());
        session.inflight.emplace(packetId, std::move(next));
        ++counters_.messagesDelivered;
    }
    if (session.queue.empty()) {
        session.queueOverflowReported = false;
    }
}

void BrokerEngine::resendInflight(Session& session) {
    // MQTT-4.4.0-1: unacknowledged messages are re-sent with the same id and DUP = 1.
    for (const auto& [packetId, delivery] : session.inflight) {
        PublishEncoder encoder(*delivery.message, delivery.retain);
        const std::vector<std::uint8_t>& bytes = encoder.atLeastOnce(packetId, true);
        transport_.send(*session.connection, bytes.data(), bytes.size());
    }
}

std::uint16_t BrokerEngine::allocatePacketId(Session& session) {
    // Terminates quickly: the in-flight window is far smaller than the 65535 id space.
    for (;;) {
        const std::uint16_t id = session.nextPacketId;
        session.nextPacketId = static_cast<std::uint16_t>(id == 0xFFFF ? 1 : id + 1);
        if (session.inflight.count(id) == 0) {
            return id;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------------------------

BrokerEngine::Session& BrokerEngine::createSession(const std::string& clientId, bool cleanSession) {
    const SessionId id = nextSessionId_++;
    Session& session = sessions_[id];
    session.id = id;
    session.clientId = clientId;
    session.cleanSession = cleanSession;
    sessionsByClientId_[clientId] = id;
    return session;
}

void BrokerEngine::destroySession(SessionId id) {
    const auto it = sessions_.find(id);
    if (it == sessions_.end()) {
        return;
    }
    for (const auto& subscription : it->second.subscriptions) {
        subscriptions_.unsubscribe(subscription.first, id);
    }
    sessionsByClientId_.erase(it->second.clientId);
    sessions_.erase(it);
}

std::string BrokerEngine::generateClientId() {
    std::string id;
    do {
        id = "pocket-" + std::to_string(++generatedClientIds_);
    } while (sessionsByClientId_.count(id) != 0);
    return id;
}

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

void BrokerEngine::sendPacket(ConnectionId id, const std::vector<std::uint8_t>& bytes) {
    transport_.send(id, bytes.data(), bytes.size());
}

void BrokerEngine::closeConnection(ClientConnection& client, const char* reason) {
    if (client.closing) {
        return;
    }
    client.closing = true;
    client.closeReason = reason;
    transport_.close(client.id);
}

std::string BrokerEngine::describe(const ClientConnection& client) {
    if (client.clientId.empty()) {
        return client.peer;
    }
    return "'" + client.clientId + "' (" + client.peer + ")";
}

BrokerStats BrokerEngine::stats() const {
    BrokerStats result = counters_;
    result.sessions = sessions_.size();
    result.clientsConnected = static_cast<std::size_t>(
        std::count_if(sessions_.begin(), sessions_.end(),
                      [](const auto& entry) { return entry.second.connection.has_value(); }));
    result.subscriptions = subscriptions_.size();
    result.retainedMessages = retained_.size();
    return result;
}

void BrokerEngine::logStats() {
    const BrokerStats current = stats();
    logInfo("stats", current.clientsConnected, " clients, ", current.sessions, " sessions, ",
            current.subscriptions, " subscriptions, ", current.retainedMessages, " retained | ",
            "in: ", current.messagesReceived, ", out: ", current.messagesDelivered,
            ", dropped: ", current.messagesDropped);
}

}  // namespace pocketmqtt
