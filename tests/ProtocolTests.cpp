#include "MqttProtocol.hpp"
#include "TestFramework.hpp"

#include <cstdint>
#include <iterator>
#include <optional>
#include <string>
#include <vector>

using namespace pocketmqtt::mqtt;

namespace {

using Bytes = std::vector<std::uint8_t>;

Bytes bytesOf(const std::string& text) { return Bytes(text.begin(), text.end()); }

/// Feeds `wire` into a fresh decoder and returns the status of the first `next()` call.
DecodeStatus decodeOne(const Bytes& wire, Packet& out, Role role = Role::Server,
                       std::size_t maxPacketSize = 1024 * 1024) {
    FrameDecoder decoder(role, maxPacketSize);
    decoder.feed(wire.data(), wire.size());
    return decoder.next(out);
}

}  // namespace

TEST_CASE(remaining_length_round_trips_at_every_boundary) {
    const std::uint32_t values[] = {0,       1,         127,       128,      16'383,
                                    16'384,  2'097'151, 2'097'152, 268'435'455};
    const std::size_t expectedSizes[] = {1, 1, 1, 2, 2, 3, 3, 4, 4};
    for (std::size_t i = 0; i < std::size(values); ++i) {
        Bytes encoded;
        encodeRemainingLength(values[i], encoded);
        CHECK_EQ(encoded.size(), expectedSizes[i]);

        const RemainingLength decoded = decodeRemainingLength(encoded.data(), encoded.size());
        CHECK_EQ(decoded.status, DecodeStatus::Ok);
        CHECK_EQ(decoded.value, values[i]);
        CHECK_EQ(decoded.bytesUsed, encoded.size());
    }
}

TEST_CASE(remaining_length_rejects_five_byte_encoding) {
    const Bytes tooLong = {0xFF, 0xFF, 0xFF, 0xFF, 0x01};
    CHECK_EQ(decodeRemainingLength(tooLong.data(), tooLong.size()).status,
             DecodeStatus::MalformedPacket);

    const Bytes truncated = {0x80, 0x80};
    CHECK_EQ(decodeRemainingLength(truncated.data(), truncated.size()).status,
             DecodeStatus::NeedMoreData);
}

TEST_CASE(utf8_validation_follows_mqtt_rules) {
    CHECK(isValidMqttUtf8("sensors/kitchen/temp"));
    CHECK(isValidMqttUtf8("ev/\xC3\xA7\xC4\xB1\xC3\xA7" "ek"));  // "ev/çiçek"
    CHECK(isValidMqttUtf8("\xF0\x9F\x8C\xA1"));               // 4-byte emoji
    CHECK(!isValidMqttUtf8(std::string("a\0b", 3)));          // U+0000 is forbidden
    CHECK(!isValidMqttUtf8("\xC0\xAF"));                      // overlong '/'
    CHECK(!isValidMqttUtf8("\xED\xA0\x80"));                  // UTF-16 surrogate
    CHECK(!isValidMqttUtf8("\xE2\x82"));                      // truncated sequence
}

TEST_CASE(connect_round_trip_with_will_and_credentials) {
    ConnectPacket original;
    original.clientId = "sensor-42";
    original.cleanSession = false;
    original.keepAliveSeconds = 30;
    original.will = Will{"sensors/42/status", bytesOf("offline"), QoS::AtLeastOnce, true};
    original.username = "eren";
    original.password = bytesOf("s3cret");

    Packet packet;
    REQUIRE(decodeOne(encodeConnect(original), packet) == DecodeStatus::Ok);
    const auto* connect = std::get_if<ConnectPacket>(&packet);
    REQUIRE(connect != nullptr);
    CHECK_EQ(connect->protocolName, std::string("MQTT"));
    CHECK_EQ(connect->protocolLevel, kProtocolLevel311);
    CHECK_EQ(connect->clientId, std::string("sensor-42"));
    CHECK_EQ(connect->cleanSession, false);
    CHECK_EQ(connect->keepAliveSeconds, 30);
    REQUIRE(connect->will.has_value());
    CHECK_EQ(connect->will->topic, std::string("sensors/42/status"));
    CHECK(connect->will->payload == bytesOf("offline"));
    CHECK_EQ(connect->will->qos, QoS::AtLeastOnce);
    CHECK_EQ(connect->will->retain, true);
    CHECK(connect->username == std::optional<std::string>("eren"));
    CHECK(connect->password == std::optional<Bytes>(bytesOf("s3cret")));
}

TEST_CASE(connect_matches_reference_bytes) {
    // CONNECT, clean session, keep-alive 60, client id "abc" - as produced by common clients.
    const Bytes wire = {0x10, 0x0F, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04,
                        0x02, 0x00, 0x3C, 0x00, 0x03, 'a', 'b', 'c'};
    ConnectPacket packet;
    packet.clientId = "abc";
    CHECK(encodeConnect(packet) == wire);
}

TEST_CASE(connect_rejects_password_without_username) {
    // Flags 0x42: clean session + password, but no username flag.
    const Bytes wire = {0x10, 0x10, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x42,
                        0x00, 0x3C, 0x00, 0x00, 0x00, 0x02, 'p', 'w'};
    Packet packet;
    CHECK_EQ(decodeOne(wire, packet), DecodeStatus::MalformedPacket);
}

TEST_CASE(connect_rejects_reserved_flag) {
    const Bytes wire = {0x10, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'T',
                        0x04, 0x03, 0x00, 0x3C, 0x00, 0x00};
    Packet packet;
    CHECK_EQ(decodeOne(wire, packet), DecodeStatus::MalformedPacket);
}

TEST_CASE(publish_round_trip_for_each_qos) {
    for (const QoS qos : {QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce}) {
        PublishPacket original;
        original.topic = "sensors/kitchen/temp";
        original.payload = bytesOf("21.5");
        original.qos = qos;
        original.retain = true;
        original.packetId = qos == QoS::AtMostOnce ? 0 : 7;

        Packet packet;
        REQUIRE(decodeOne(encodePublish(original), packet) == DecodeStatus::Ok);
        const auto* publish = std::get_if<PublishPacket>(&packet);
        REQUIRE(publish != nullptr);
        CHECK_EQ(publish->topic, original.topic);
        CHECK(publish->payload == original.payload);
        CHECK_EQ(publish->qos, qos);
        CHECK_EQ(publish->retain, true);
        CHECK_EQ(publish->packetId, original.packetId);
    }
}

TEST_CASE(publish_rejects_invalid_headers_and_topics) {
    Packet packet;
    // QoS 3 is reserved.
    CHECK_EQ(decodeOne({0x36, 0x05, 0x00, 0x01, 'a', 0x00, 0x01}, packet),
             DecodeStatus::MalformedPacket);
    // DUP must be 0 for QoS 0.
    CHECK_EQ(decodeOne({0x38, 0x03, 0x00, 0x01, 'a'}, packet), DecodeStatus::MalformedPacket);
    // Topic names must not contain wildcards.
    CHECK_EQ(decodeOne({0x30, 0x05, 0x00, 0x03, 'a', '/', '#'}, packet),
             DecodeStatus::MalformedPacket);
    // QoS 1 requires a non-zero packet identifier.
    CHECK_EQ(decodeOne({0x32, 0x05, 0x00, 0x01, 'a', 0x00, 0x00}, packet),
             DecodeStatus::MalformedPacket);
}

TEST_CASE(subscribe_round_trip_and_flag_validation) {
    SubscribePacket original;
    original.packetId = 10;
    original.requests = {{"sensors/+/temp", QoS::AtLeastOnce}, {"alerts/#", QoS::AtMostOnce}};
    const Bytes wire = encodeSubscribe(original);

    Packet packet;
    REQUIRE(decodeOne(wire, packet) == DecodeStatus::Ok);
    const auto* subscribe = std::get_if<SubscribePacket>(&packet);
    REQUIRE(subscribe != nullptr);
    CHECK_EQ(subscribe->packetId, 10);
    REQUIRE(subscribe->requests.size() == 2);
    CHECK_EQ(subscribe->requests[0].filter, std::string("sensors/+/temp"));
    CHECK_EQ(subscribe->requests[0].qos, QoS::AtLeastOnce);
    CHECK_EQ(subscribe->requests[1].filter, std::string("alerts/#"));

    Bytes wrongFlags = wire;
    wrongFlags[0] = 0x80;  // SUBSCRIBE must carry flags 0b0010.
    CHECK_EQ(decodeOne(wrongFlags, packet), DecodeStatus::MalformedPacket);

    // A SUBSCRIBE without any topic filter is a protocol violation.
    CHECK_EQ(decodeOne({0x82, 0x02, 0x00, 0x01}, packet), DecodeStatus::ProtocolError);
}

TEST_CASE(decoder_handles_byte_by_byte_fragmentation) {
    PublishPacket publish;
    publish.topic = "a/b";
    publish.payload = bytesOf("hello");
    const Bytes wire = encodePublish(publish);

    FrameDecoder decoder(Role::Server);
    Packet packet;
    for (std::size_t i = 0; i + 1 < wire.size(); ++i) {
        decoder.feed(&wire[i], 1);
        CHECK_EQ(decoder.next(packet), DecodeStatus::NeedMoreData);
    }
    decoder.feed(&wire.back(), 1);
    REQUIRE(decoder.next(packet) == DecodeStatus::Ok);
    CHECK(std::get<PublishPacket>(packet).payload == bytesOf("hello"));
    CHECK_EQ(decoder.bufferedBytes(), 0u);
}

TEST_CASE(decoder_splits_coalesced_packets) {
    Bytes wire = encodePingreq();
    const Bytes ack = encodeAck({PacketType::Puback, 99});
    wire.insert(wire.end(), ack.begin(), ack.end());
    const Bytes disconnect = encodeDisconnect();
    wire.insert(wire.end(), disconnect.begin(), disconnect.end());

    FrameDecoder decoder(Role::Server);
    decoder.feed(wire.data(), wire.size());
    Packet packet;
    REQUIRE(decoder.next(packet) == DecodeStatus::Ok);
    CHECK(std::holds_alternative<PingreqPacket>(packet));
    REQUIRE(decoder.next(packet) == DecodeStatus::Ok);
    CHECK_EQ(std::get<AckPacket>(packet).packetId, 99);
    REQUIRE(decoder.next(packet) == DecodeStatus::Ok);
    CHECK(std::holds_alternative<DisconnectPacket>(packet));
    CHECK_EQ(decoder.next(packet), DecodeStatus::NeedMoreData);
}

TEST_CASE(decoder_rejects_oversized_packets_from_header_alone) {
    // Only the fixed header of a 1 MiB PUBLISH has arrived; the limit is 1 KiB.
    Bytes header = {0x30};
    encodeRemainingLength(1024 * 1024, header);
    Packet packet;
    CHECK_EQ(decodeOne(header, packet, Role::Server, 1024), DecodeStatus::PacketTooLarge);
}

TEST_CASE(decoder_enforces_packet_direction) {
    Packet packet;
    const Bytes connack = encodeConnack({false, ConnectReturnCode::Accepted});
    CHECK_EQ(decodeOne(connack, packet, Role::Server), DecodeStatus::ProtocolError);
    CHECK_EQ(decodeOne(connack, packet, Role::Client), DecodeStatus::Ok);

    const Bytes subscribe = encodeSubscribe({1, {{"x", QoS::AtMostOnce}}});
    CHECK_EQ(decodeOne(subscribe, packet, Role::Client), DecodeStatus::ProtocolError);
}

TEST_CASE(suback_and_unsuback_round_trip) {
    Packet packet;
    REQUIRE(decodeOne(encodeSuback({5, {0x00, 0x01, kSubackFailure}}), packet, Role::Client) ==
            DecodeStatus::Ok);
    const auto& suback = std::get<SubackPacket>(packet);
    CHECK_EQ(suback.packetId, 5);
    CHECK(suback.returnCodes == Bytes({0x00, 0x01, kSubackFailure}));

    REQUIRE(decodeOne(encodeAck({PacketType::Unsuback, 6}), packet, Role::Client) ==
            DecodeStatus::Ok);
    CHECK_EQ(packetTypeOf(packet), PacketType::Unsuback);
}
