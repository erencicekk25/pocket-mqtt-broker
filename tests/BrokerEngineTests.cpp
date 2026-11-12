#include "BrokerEngine.hpp"
#include "TestFramework.hpp"

#include <chrono>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace pocketmqtt;
using namespace pocketmqtt::mqtt;

namespace {

using Bytes = std::vector<std::uint8_t>;

Bytes bytesOf(const std::string& text) { return Bytes(text.begin(), text.end()); }

/// In-memory transport: decodes everything the broker sends, as a client would.
class FakeTransport final : public ITransport {
public:
    bool send(ConnectionId id, const std::uint8_t* data, std::size_t size) override {
        if (closeRequested.count(id) != 0) return false;
        FrameDecoder& decoder = decoders_.try_emplace(id, Role::Client, 1u << 24).first->second;
        decoder.feed(data, size);
        Packet packet;
        while (decoder.next(packet) == DecodeStatus::Ok) {
            inbox[id].push_back(packet);
        }
        return true;
    }

    void close(ConnectionId id) override { closeRequested.insert(id); }

    std::unordered_map<ConnectionId, std::vector<Packet>> inbox;
    std::unordered_set<ConnectionId> closeRequested;

private:
    std::unordered_map<ConnectionId, FrameDecoder> decoders_;
};

/// Drives a BrokerEngine the way TcpServer would, without any sockets.
class Harness {
public:
    explicit Harness(BrokerConfig config = {}) : broker(transport, config) {}

    ConnectionId open() {
        const ConnectionId id = nextId_++;
        broker.onAccept(id, "127.0.0.1:" + std::to_string(50000 + id));
        return id;
    }

    void send(ConnectionId id, const Bytes& bytes) {
        broker.onData(id, bytes.data(), bytes.size());
        reapClosed();
    }

    ConnectionId connect(const std::string& clientId, bool cleanSession = true,
                         std::optional<Will> will = std::nullopt, std::uint16_t keepAlive = 60) {
        const ConnectionId id = open();
        ConnectPacket connect;
        connect.clientId = clientId;
        connect.cleanSession = cleanSession;
        connect.keepAliveSeconds = keepAlive;
        connect.will = std::move(will);
        send(id, encodeConnect(connect));
        return id;
    }

    void subscribe(ConnectionId id, const std::string& filter, QoS qos = QoS::AtMostOnce) {
        send(id, encodeSubscribe({nextPacketId_++, {{filter, qos}}}));
    }

    void publish(ConnectionId id, const std::string& topic, const std::string& payload,
                 QoS qos = QoS::AtMostOnce, bool retain = false) {
        PublishPacket packet;
        packet.topic = topic;
        packet.payload = bytesOf(payload);
        packet.qos = qos;
        packet.retain = retain;
        packet.packetId = qos == QoS::AtMostOnce ? 0 : nextPacketId_++;
        send(id, encodePublish(packet));
    }

    /// Simulates the network dropping the connection (no DISCONNECT).
    void dropConnection(ConnectionId id) {
        transport.closeRequested.insert(id);
        broker.onClose(id);
        notified_.insert(id);
    }

    void tick(std::chrono::seconds ahead) {
        broker.onTick(Clock::now() + ahead);
        reapClosed();
    }

    /// Returns and clears everything the broker sent to `id`.
    std::vector<Packet> take(ConnectionId id) {
        std::vector<Packet> packets = std::move(transport.inbox[id]);
        transport.inbox[id].clear();
        return packets;
    }

    template <typename T>
    std::vector<T> takeOf(ConnectionId id) {
        std::vector<T> result;
        for (Packet& packet : take(id)) {
            if (auto* value = std::get_if<T>(&packet)) result.push_back(std::move(*value));
        }
        return result;
    }

    bool isClosed(ConnectionId id) const { return transport.closeRequested.count(id) != 0; }

    FakeTransport transport;
    BrokerEngine broker;

private:
    // Mirrors TcpServer::reap(): every close request ends in exactly one onClose().
    void reapClosed() {
        for (const ConnectionId id : std::vector<ConnectionId>(transport.closeRequested.begin(),
                                                               transport.closeRequested.end())) {
            if (notified_.insert(id).second) broker.onClose(id);
        }
    }

    ConnectionId nextId_ = 1;
    std::uint16_t nextPacketId_ = 1;
    std::unordered_set<ConnectionId> notified_;
};

std::string payloadText(const PublishPacket& packet) {
    return std::string(packet.payload.begin(), packet.payload.end());
}

}  // namespace

TEST_CASE(broker_accepts_connect_with_connack) {
    Harness h;
    const ConnectionId client = h.connect("sensor-1");
    const auto acks = h.takeOf<ConnackPacket>(client);
    REQUIRE(acks.size() == 1);
    CHECK_EQ(acks[0].returnCode, ConnectReturnCode::Accepted);
    CHECK_EQ(acks[0].sessionPresent, false);
    CHECK_EQ(h.broker.stats().clientsConnected, 1u);
}

TEST_CASE(broker_routes_single_level_wildcards) {
    Harness h;
    const ConnectionId subscriber = h.connect("dashboard");
    const ConnectionId publisher = h.connect("sensor");
    h.subscribe(subscriber, "sensors/+/temp");
    h.take(subscriber);

    h.publish(publisher, "sensors/kitchen/temp", "21.5");
    h.publish(publisher, "sensors/kitchen/humidity", "40");
    h.publish(publisher, "sensors/garage/temp", "12.0");

    const auto received = h.takeOf<PublishPacket>(subscriber);
    REQUIRE(received.size() == 2);
    CHECK_EQ(received[0].topic, std::string("sensors/kitchen/temp"));
    CHECK_EQ(payloadText(received[0]), std::string("21.5"));
    CHECK_EQ(received[1].topic, std::string("sensors/garage/temp"));
}

TEST_CASE(broker_delivers_overlapping_matches_once_with_max_qos) {
    Harness h;
    const ConnectionId subscriber = h.connect("sub");
    const ConnectionId publisher = h.connect("pub");
    h.subscribe(subscriber, "home/#", QoS::AtMostOnce);
    h.subscribe(subscriber, "home/+/light", QoS::AtLeastOnce);
    h.take(subscriber);

    h.publish(publisher, "home/kitchen/light", "on", QoS::AtLeastOnce);
    const auto received = h.takeOf<PublishPacket>(subscriber);
    REQUIRE(received.size() == 1);
    CHECK_EQ(received[0].qos, QoS::AtLeastOnce);
}

TEST_CASE(broker_qos1_flow_acknowledges_and_tracks_inflight) {
    BrokerConfig config;
    config.maxInflightMessages = 2;
    Harness h(config);
    const ConnectionId subscriber = h.connect("sub");
    const ConnectionId publisher = h.connect("pub");
    h.subscribe(subscriber, "jobs", QoS::AtLeastOnce);
    h.take(subscriber);
    h.take(publisher);

    for (int i = 0; i < 4; ++i) h.publish(publisher, "jobs", "job-" + std::to_string(i), QoS::AtLeastOnce);
    CHECK_EQ(h.takeOf<AckPacket>(publisher).size(), 4u);  // Every publish gets a PUBACK.

    auto received = h.takeOf<PublishPacket>(subscriber);
    REQUIRE(received.size() == 2);  // In-flight window is full; the rest is queued.
    CHECK_EQ(payloadText(received[0]), std::string("job-0"));
    CHECK(received[0].packetId != received[1].packetId);

    h.send(subscriber, encodeAck({PacketType::Puback, received[0].packetId}));
    received = h.takeOf<PublishPacket>(subscriber);
    REQUIRE(received.size() == 1);
    CHECK_EQ(payloadText(received[0]), std::string("job-2"));
}

TEST_CASE(broker_qos2_inbound_is_routed_exactly_once) {
    Harness h;
    const ConnectionId subscriber = h.connect("sub");
    const ConnectionId publisher = h.connect("pub");
    h.subscribe(subscriber, "billing");
    h.take(subscriber);
    h.take(publisher);

    PublishPacket packet{"billing", bytesOf("invoice-1"), QoS::ExactlyOnce, false, false, 77};
    h.send(publisher, encodePublish(packet));
    packet.dup = true;
    h.send(publisher, encodePublish(packet));  // Retransmission before PUBREL.

    const auto acks = h.takeOf<AckPacket>(publisher);
    REQUIRE(acks.size() == 2);
    CHECK_EQ(acks[0].type, PacketType::Pubrec);
    CHECK_EQ(h.takeOf<PublishPacket>(subscriber).size(), 1u);

    h.send(publisher, encodeAck({PacketType::Pubrel, 77}));
    const auto completion = h.takeOf<AckPacket>(publisher);
    REQUIRE(completion.size() == 1);
    CHECK_EQ(completion[0].type, PacketType::Pubcomp);
}

TEST_CASE(broker_retained_messages_are_replayed_and_cleared) {
    Harness h;
    const ConnectionId publisher = h.connect("pub");
    h.publish(publisher, "status/door", "open", QoS::AtMostOnce, true);

    const ConnectionId late = h.connect("late");
    h.subscribe(late, "status/+");
    auto received = h.takeOf<PublishPacket>(late);
    REQUIRE(received.size() == 1);
    CHECK_EQ(payloadText(received[0]), std::string("open"));
    CHECK_EQ(received[0].retain, true);

    h.publish(publisher, "status/door", "closed", QoS::AtMostOnce, true);
    received = h.takeOf<PublishPacket>(late);
    REQUIRE(received.size() == 1);
    CHECK_EQ(received[0].retain, false);  // Live deliveries never carry RETAIN.

    h.publish(publisher, "status/door", "", QoS::AtMostOnce, true);  // Clears the retained slot.
    CHECK_EQ(h.broker.stats().retainedMessages, 0u);
}

TEST_CASE(broker_publishes_will_only_on_unexpected_disconnect) {
    Harness h;
    const ConnectionId watcher = h.connect("watcher");
    h.subscribe(watcher, "devices/+/status");
    h.take(watcher);

    const Will will{"devices/cam/status", bytesOf("offline"), QoS::AtMostOnce, false};
    const ConnectionId camera = h.connect("cam", true, will);
    h.dropConnection(camera);
    auto received = h.takeOf<PublishPacket>(watcher);
    REQUIRE(received.size() == 1);
    CHECK_EQ(payloadText(received[0]), std::string("offline"));

    const ConnectionId polite = h.connect("cam", true, will);
    h.send(polite, encodeDisconnect());
    CHECK(h.isClosed(polite));
    CHECK(h.takeOf<PublishPacket>(watcher).empty());
}

TEST_CASE(broker_client_id_takeover_closes_previous_connection) {
    Harness h;
    const ConnectionId first = h.connect("sensor");
    const ConnectionId second = h.connect("sensor");
    CHECK(h.isClosed(first));
    CHECK(!h.isClosed(second));
    CHECK_EQ(h.broker.stats().clientsConnected, 1u);
    CHECK_EQ(h.broker.stats().sessions, 1u);
}

TEST_CASE(broker_persistent_session_queues_qos1_while_offline) {
    Harness h;
    ConnectionId worker = h.connect("worker", false);
    h.subscribe(worker, "tasks/#", QoS::AtLeastOnce);
    h.dropConnection(worker);

    const ConnectionId producer = h.connect("producer");
    h.publish(producer, "tasks/a", "1", QoS::AtLeastOnce);
    h.publish(producer, "tasks/b", "2", QoS::AtMostOnce);  // QoS 0 is not stored offline.
    h.publish(producer, "tasks/c", "3", QoS::AtLeastOnce);

    worker = h.connect("worker", false);
    const auto packets = h.take(worker);
    REQUIRE(!packets.empty());
    const auto* connack = std::get_if<ConnackPacket>(&packets[0]);
    REQUIRE(connack != nullptr);
    CHECK_EQ(connack->sessionPresent, true);

    std::vector<std::string> payloads;
    for (const Packet& packet : packets) {
        if (const auto* publish = std::get_if<PublishPacket>(&packet)) payloads.push_back(payloadText(*publish));
    }
    CHECK(payloads == std::vector<std::string>({"1", "3"}));
}

TEST_CASE(broker_clean_session_discards_state) {
    Harness h;
    const ConnectionId worker = h.connect("worker", false);
    h.subscribe(worker, "tasks/#", QoS::AtLeastOnce);
    h.dropConnection(worker);

    const ConnectionId again = h.connect("worker", true);
    CHECK_EQ(h.takeOf<ConnackPacket>(again).at(0).sessionPresent, false);
    CHECK_EQ(h.broker.stats().subscriptions, 0u);
}

TEST_CASE(broker_enforces_keep_alive_and_publishes_will) {
    Harness h;
    const ConnectionId watcher = h.connect("watcher", true, std::nullopt, 0);
    h.subscribe(watcher, "lwt/#");
    h.take(watcher);

    const ConnectionId sleepy =
        h.connect("sleepy", true, Will{"lwt/sleepy", bytesOf("gone"), QoS::AtMostOnce, false}, 2);
    h.tick(std::chrono::seconds(2));
    CHECK(!h.isClosed(sleepy));  // 1.5x grace period has not elapsed yet.
    h.tick(std::chrono::seconds(4));
    CHECK(h.isClosed(sleepy));
    CHECK(!h.isClosed(watcher));  // Keep-alive 0 disables the check.
    CHECK_EQ(h.takeOf<PublishPacket>(watcher).size(), 1u);
}

TEST_CASE(broker_rejects_protocol_violations) {
    Harness h;
    // A packet other than CONNECT first.
    const ConnectionId rude = h.open();
    h.send(rude, encodePingreq());
    CHECK(h.isClosed(rude));

    // Unsupported protocol level.
    const ConnectionId old = h.open();
    ConnectPacket v31;
    v31.protocolLevel = 3;
    v31.clientId = "legacy";
    h.send(old, encodeConnect(v31));
    auto acks = h.takeOf<ConnackPacket>(old);
    REQUIRE(acks.size() == 1);
    CHECK_EQ(acks[0].returnCode, ConnectReturnCode::UnacceptableProtocolVersion);
    CHECK(h.isClosed(old));

    // Empty client id requires a clean session.
    const ConnectionId anonymous = h.open();
    ConnectPacket noId;
    noId.cleanSession = false;
    h.send(anonymous, encodeConnect(noId));
    acks = h.takeOf<ConnackPacket>(anonymous);
    REQUIRE(acks.size() == 1);
    CHECK_EQ(acks[0].returnCode, ConnectReturnCode::IdentifierRejected);

    // Silent connections are dropped after the CONNECT timeout.
    const ConnectionId silent = h.open();
    h.tick(std::chrono::seconds(30));
    CHECK(h.isClosed(silent));
}

TEST_CASE(broker_subscribe_reports_invalid_filters_and_unsubscribe_stops_delivery) {
    Harness h;
    const ConnectionId client = h.connect("client");
    h.take(client);
    h.send(client, encodeSubscribe({9, {{"ok/+", QoS::ExactlyOnce}, {"bad/#/x", QoS::AtMostOnce}}}));
    const auto subacks = h.takeOf<SubackPacket>(client);
    REQUIRE(subacks.size() == 1);
    // QoS 2 requests are granted QoS 1; invalid filters get 0x80.
    CHECK(subacks[0].returnCodes == Bytes({0x01, kSubackFailure}));

    h.send(client, encodeUnsubscribe({10, {"ok/+"}}));
    CHECK_EQ(h.takeOf<AckPacket>(client).at(0).type, PacketType::Unsuback);
    h.publish(client, "ok/x", "ignored");
    CHECK(h.takeOf<PublishPacket>(client).empty());
}

TEST_CASE(broker_answers_pingreq_and_assigns_client_ids) {
    Harness h;
    const ConnectionId client = h.connect("");  // Server assigns an id for clean sessions.
    h.take(client);
    h.send(client, encodePingreq());
    const auto packets = h.take(client);
    REQUIRE(packets.size() == 1);
    CHECK(std::holds_alternative<PingrespPacket>(packets[0]));
}

TEST_CASE(broker_hides_system_topics_from_root_wildcards) {
    Harness h;
    const ConnectionId everything = h.connect("everything");
    h.subscribe(everything, "#");
    h.take(everything);
    const ConnectionId publisher = h.connect("pub");
    h.publish(publisher, "$SYS/broker/uptime", "42");
    CHECK(h.takeOf<PublishPacket>(everything).empty());
}
