#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

/// MQTT 3.1.1 (OASIS standard, protocol level 4) wire format: decoding and encoding of
/// control packets. This module is pure: no I/O, no global state, no allocations beyond
/// the packets it produces.
namespace pocketmqtt::mqtt {

inline constexpr std::uint8_t kProtocolLevel311 = 4;
inline constexpr std::string_view kProtocolName = "MQTT";
/// Largest value representable by the 4-byte "Remaining Length" varint (256 MiB - 1).
inline constexpr std::uint32_t kMaxRemainingLength = 268'435'455;

enum class PacketType : std::uint8_t {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
};

enum class QoS : std::uint8_t { AtMostOnce = 0, AtLeastOnce = 1, ExactlyOnce = 2 };

enum class ConnectReturnCode : std::uint8_t {
    Accepted = 0,
    UnacceptableProtocolVersion = 1,
    IdentifierRejected = 2,
    ServerUnavailable = 3,
    BadUsernameOrPassword = 4,
    NotAuthorized = 5,
};

/// SUBACK return code signalling a rejected subscription.
inline constexpr std::uint8_t kSubackFailure = 0x80;

/// Which end of the connection we decode for. A server only accepts client-to-server
/// packets (and vice versa); anything else is a protocol violation.
enum class Role : std::uint8_t { Server, Client };

enum class DecodeStatus : std::uint8_t {
    Ok,
    NeedMoreData,     ///< Not an error: wait for more bytes.
    MalformedPacket,  ///< Violates the wire format; the connection must be closed.
    ProtocolError,    ///< Well-formed, but not allowed here (e.g. CONNACK sent to a server).
    PacketTooLarge,   ///< Exceeds the configured maximum packet size.
};

struct Will {
    std::string topic;
    std::vector<std::uint8_t> payload;
    QoS qos = QoS::AtMostOnce;
    bool retain = false;
};

struct ConnectPacket {
    std::string protocolName{kProtocolName};
    std::uint8_t protocolLevel = kProtocolLevel311;
    bool cleanSession = true;
    std::uint16_t keepAliveSeconds = 60;
    std::string clientId;
    std::optional<Will> will;
    std::optional<std::string> username;
    std::optional<std::vector<std::uint8_t>> password;
};

struct ConnackPacket {
    bool sessionPresent = false;
    ConnectReturnCode returnCode = ConnectReturnCode::Accepted;
};

struct PublishPacket {
    std::string topic;
    std::vector<std::uint8_t> payload;
    QoS qos = QoS::AtMostOnce;
    bool retain = false;
    bool dup = false;
    std::uint16_t packetId = 0;  ///< Only meaningful for QoS 1 and 2.
};

/// PUBACK, PUBREC, PUBREL, PUBCOMP and UNSUBACK: a type plus a packet identifier.
struct AckPacket {
    PacketType type = PacketType::Puback;
    std::uint16_t packetId = 0;
};

struct SubscriptionRequest {
    std::string filter;
    QoS qos = QoS::AtMostOnce;
};

struct SubscribePacket {
    std::uint16_t packetId = 0;
    std::vector<SubscriptionRequest> requests;
};

struct SubackPacket {
    std::uint16_t packetId = 0;
    std::vector<std::uint8_t> returnCodes;  ///< Granted QoS (0-2) or kSubackFailure.
};

struct UnsubscribePacket {
    std::uint16_t packetId = 0;
    std::vector<std::string> filters;
};

struct PingreqPacket {};
struct PingrespPacket {};
struct DisconnectPacket {};

using Packet = std::variant<ConnectPacket, ConnackPacket, PublishPacket, AckPacket, SubscribePacket,
                            SubackPacket, UnsubscribePacket, PingreqPacket, PingrespPacket,
                            DisconnectPacket>;

[[nodiscard]] const char* toString(PacketType type) noexcept;
[[nodiscard]] const char* toString(DecodeStatus status) noexcept;
[[nodiscard]] const char* toString(ConnectReturnCode code) noexcept;
[[nodiscard]] PacketType packetTypeOf(const Packet& packet) noexcept;

// ---------------------------------------------------------------------------------------------
// Low-level building blocks
// ---------------------------------------------------------------------------------------------

struct RemainingLength {
    DecodeStatus status = DecodeStatus::NeedMoreData;
    std::uint32_t value = 0;
    std::size_t bytesUsed = 0;
};

/// Decodes the variable-length "Remaining Length" field (1-4 bytes, 7 bits each).
[[nodiscard]] RemainingLength decodeRemainingLength(const std::uint8_t* data,
                                                    std::size_t size) noexcept;

/// Appends the varint encoding of `value` (must be <= kMaxRemainingLength) to `out`.
void encodeRemainingLength(std::uint32_t value, std::vector<std::uint8_t>& out);

/// Well-formed UTF-8 as required for MQTT strings: no overlongs, no surrogates, no U+0000.
[[nodiscard]] bool isValidMqttUtf8(std::string_view text) noexcept;

/// Decodes one packet given its first header byte and its complete variable header + payload.
[[nodiscard]] DecodeStatus decodePacket(std::uint8_t headerByte, const std::uint8_t* body,
                                        std::size_t size, Role role, Packet& out);

// ---------------------------------------------------------------------------------------------
// Stream framing
// ---------------------------------------------------------------------------------------------

/// Incremental decoder that turns an arbitrary-fragmented TCP byte stream into packets.
///
/// Usage: `feed()` whatever arrived, then call `next()` until it returns NeedMoreData.
/// Any other non-Ok status is terminal for the connection.
class FrameDecoder {
public:
    explicit FrameDecoder(Role role, std::size_t maxPacketSize = 1024 * 1024) noexcept
        : role_(role), maxPacketSize_(maxPacketSize) {}

    void feed(const std::uint8_t* data, std::size_t size);
    [[nodiscard]] DecodeStatus next(Packet& out);
    [[nodiscard]] std::size_t bufferedBytes() const noexcept { return buffer_.size() - readOffset_; }

private:
    Role role_;
    std::size_t maxPacketSize_;
    std::vector<std::uint8_t> buffer_;
    std::size_t readOffset_ = 0;
};

// ---------------------------------------------------------------------------------------------
// Encoders
// ---------------------------------------------------------------------------------------------

[[nodiscard]] std::vector<std::uint8_t> encodeConnect(const ConnectPacket& packet);
[[nodiscard]] std::vector<std::uint8_t> encodeConnack(const ConnackPacket& packet);
[[nodiscard]] std::vector<std::uint8_t> encodePublish(const PublishPacket& packet);
/// PUBACK, PUBREC, PUBREL, PUBCOMP or UNSUBACK.
[[nodiscard]] std::vector<std::uint8_t> encodeAck(const AckPacket& packet);
[[nodiscard]] std::vector<std::uint8_t> encodeSubscribe(const SubscribePacket& packet);
[[nodiscard]] std::vector<std::uint8_t> encodeSuback(const SubackPacket& packet);
[[nodiscard]] std::vector<std::uint8_t> encodeUnsubscribe(const UnsubscribePacket& packet);
[[nodiscard]] std::vector<std::uint8_t> encodePingreq();
[[nodiscard]] std::vector<std::uint8_t> encodePingresp();
[[nodiscard]] std::vector<std::uint8_t> encodeDisconnect();

/// Encodes any packet held in the variant.
[[nodiscard]] std::vector<std::uint8_t> encode(const Packet& packet);

}  // namespace pocketmqtt::mqtt
