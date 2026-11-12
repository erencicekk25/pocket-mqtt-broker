#include "MqttProtocol.hpp"

#include <stdexcept>
#include <type_traits>
#include <utility>

namespace pocketmqtt::mqtt {
namespace {

// ---------------------------------------------------------------------------------------------
// Decoding helpers
// ---------------------------------------------------------------------------------------------

/// Bounds-checked big-endian reader over one packet body.
class Reader {
public:
    Reader(const std::uint8_t* data, std::size_t size) noexcept : data_(data), size_(size) {}

    [[nodiscard]] std::size_t remaining() const noexcept { return size_ - position_; }
    [[nodiscard]] bool atEnd() const noexcept { return position_ == size_; }

    bool readByte(std::uint8_t& out) noexcept {
        if (remaining() < 1) return false;
        out = data_[position_++];
        return true;
    }

    bool readU16(std::uint16_t& out) noexcept {
        if (remaining() < 2) return false;
        out = static_cast<std::uint16_t>((data_[position_] << 8) | data_[position_ + 1]);
        position_ += 2;
        return true;
    }

    /// Two-byte length prefix followed by raw bytes.
    bool readBinary(std::vector<std::uint8_t>& out) {
        std::uint16_t length = 0;
        if (!readU16(length) || remaining() < length) return false;
        out.assign(data_ + position_, data_ + position_ + length);
        position_ += length;
        return true;
    }

    /// Two-byte length prefix followed by MQTT-flavoured UTF-8.
    bool readString(std::string& out) {
        std::uint16_t length = 0;
        if (!readU16(length) || remaining() < length) return false;
        out.assign(reinterpret_cast<const char*>(data_ + position_), length);
        position_ += length;
        return isValidMqttUtf8(out);
    }

    /// Everything that is left, e.g. a PUBLISH payload.
    void readRest(std::vector<std::uint8_t>& out) {
        out.assign(data_ + position_, data_ + size_);
        position_ = size_;
    }

private:
    const std::uint8_t* data_;
    std::size_t size_;
    std::size_t position_ = 0;
};

bool isAllowedFor(Role role, PacketType type) noexcept {
    switch (type) {
        case PacketType::Publish:
        case PacketType::Puback:
        case PacketType::Pubrec:
        case PacketType::Pubrel:
        case PacketType::Pubcomp:
            return true;  // Bidirectional.
        case PacketType::Connect:
        case PacketType::Subscribe:
        case PacketType::Unsubscribe:
        case PacketType::Pingreq:
        case PacketType::Disconnect:
            return role == Role::Server;
        case PacketType::Connack:
        case PacketType::Suback:
        case PacketType::Unsuback:
        case PacketType::Pingresp:
            return role == Role::Client;
    }
    return false;
}

/// Fixed-header flag nibble required by MQTT-2.2.2 for every packet type except PUBLISH.
std::uint8_t requiredFlags(PacketType type) noexcept {
    switch (type) {
        case PacketType::Pubrel:
        case PacketType::Subscribe:
        case PacketType::Unsubscribe:
            return 0x02;
        default:
            return 0x00;
    }
}

bool containsWildcard(std::string_view topic) noexcept {
    return topic.find_first_of("+#") != std::string_view::npos;
}

DecodeStatus decodeConnect(Reader& reader, Packet& out) {
    ConnectPacket packet;
    std::uint8_t flags = 0;
    if (!reader.readString(packet.protocolName) || !reader.readByte(packet.protocolLevel) ||
        !reader.readByte(flags) || !reader.readU16(packet.keepAliveSeconds)) {
        return DecodeStatus::MalformedPacket;
    }

    const bool reserved = (flags & 0x01) != 0;
    packet.cleanSession = (flags & 0x02) != 0;
    const bool willFlag = (flags & 0x04) != 0;
    const auto willQos = static_cast<std::uint8_t>((flags >> 3) & 0x03);
    const bool willRetain = (flags & 0x20) != 0;
    const bool passwordFlag = (flags & 0x40) != 0;
    const bool usernameFlag = (flags & 0x80) != 0;

    if (reserved || willQos > 2 || (!willFlag && (willQos != 0 || willRetain)) ||
        (passwordFlag && !usernameFlag)) {
        return DecodeStatus::MalformedPacket;  // MQTT-3.1.2-3, -14, -15, -22
    }

    if (!reader.readString(packet.clientId)) {
        return DecodeStatus::MalformedPacket;
    }
    if (willFlag) {
        Will will;
        if (!reader.readString(will.topic) || !reader.readBinary(will.payload) ||
            will.topic.empty() || containsWildcard(will.topic)) {
            return DecodeStatus::MalformedPacket;
        }
        will.qos = static_cast<QoS>(willQos);
        will.retain = willRetain;
        packet.will = std::move(will);
    }
    if (usernameFlag) {
        std::string username;
        if (!reader.readString(username)) return DecodeStatus::MalformedPacket;
        packet.username = std::move(username);
    }
    if (passwordFlag) {
        std::vector<std::uint8_t> password;
        if (!reader.readBinary(password)) return DecodeStatus::MalformedPacket;
        packet.password = std::move(password);
    }
    if (!reader.atEnd()) {
        return DecodeStatus::MalformedPacket;
    }
    out = std::move(packet);
    return DecodeStatus::Ok;
}

DecodeStatus decodeConnack(Reader& reader, Packet& out) {
    std::uint8_t ackFlags = 0;
    std::uint8_t code = 0;
    if (reader.remaining() != 2 || !reader.readByte(ackFlags) || !reader.readByte(code) ||
        (ackFlags & 0xFE) != 0 || code > 5) {
        return DecodeStatus::MalformedPacket;
    }
    out = ConnackPacket{(ackFlags & 0x01) != 0, static_cast<ConnectReturnCode>(code)};
    return DecodeStatus::Ok;
}

DecodeStatus decodePublish(std::uint8_t flags, Reader& reader, Packet& out) {
    PublishPacket packet;
    packet.dup = (flags & 0x08) != 0;
    const auto qos = static_cast<std::uint8_t>((flags >> 1) & 0x03);
    packet.retain = (flags & 0x01) != 0;
    if (qos > 2 || (qos == 0 && packet.dup)) {
        return DecodeStatus::MalformedPacket;  // MQTT-3.3.1-2, -4
    }
    packet.qos = static_cast<QoS>(qos);

    if (!reader.readString(packet.topic) || packet.topic.empty() || containsWildcard(packet.topic)) {
        return DecodeStatus::MalformedPacket;  // MQTT-3.3.2-2
    }
    if (packet.qos != QoS::AtMostOnce) {
        if (!reader.readU16(packet.packetId) || packet.packetId == 0) {
            return DecodeStatus::MalformedPacket;
        }
    }
    reader.readRest(packet.payload);
    out = std::move(packet);
    return DecodeStatus::Ok;
}

DecodeStatus decodeAck(PacketType type, Reader& reader, Packet& out) {
    std::uint16_t packetId = 0;
    if (reader.remaining() != 2 || !reader.readU16(packetId) || packetId == 0) {
        return DecodeStatus::MalformedPacket;
    }
    out = AckPacket{type, packetId};
    return DecodeStatus::Ok;
}

DecodeStatus decodeSubscribe(Reader& reader, Packet& out) {
    SubscribePacket packet;
    if (!reader.readU16(packet.packetId) || packet.packetId == 0) {
        return DecodeStatus::MalformedPacket;
    }
    while (!reader.atEnd()) {
        SubscriptionRequest request;
        std::uint8_t options = 0;
        if (!reader.readString(request.filter) || !reader.readByte(options) || options > 2) {
            return DecodeStatus::MalformedPacket;  // Reserved bits set or QoS 3: MQTT-3.8.3-4
        }
        request.qos = static_cast<QoS>(options);
        packet.requests.push_back(std::move(request));
    }
    if (packet.requests.empty()) {
        return DecodeStatus::ProtocolError;  // MQTT-3.8.3-3
    }
    out = std::move(packet);
    return DecodeStatus::Ok;
}

DecodeStatus decodeSuback(Reader& reader, Packet& out) {
    SubackPacket packet;
    if (!reader.readU16(packet.packetId) || reader.atEnd()) {
        return DecodeStatus::MalformedPacket;
    }
    reader.readRest(packet.returnCodes);
    for (const std::uint8_t code : packet.returnCodes) {
        if (code > 2 && code != kSubackFailure) return DecodeStatus::MalformedPacket;
    }
    out = std::move(packet);
    return DecodeStatus::Ok;
}

DecodeStatus decodeUnsubscribe(Reader& reader, Packet& out) {
    UnsubscribePacket packet;
    if (!reader.readU16(packet.packetId) || packet.packetId == 0) {
        return DecodeStatus::MalformedPacket;
    }
    while (!reader.atEnd()) {
        std::string filter;
        if (!reader.readString(filter)) return DecodeStatus::MalformedPacket;
        packet.filters.push_back(std::move(filter));
    }
    if (packet.filters.empty()) {
        return DecodeStatus::ProtocolError;  // MQTT-3.10.3-2
    }
    out = std::move(packet);
    return DecodeStatus::Ok;
}

template <typename EmptyPacket>
DecodeStatus decodeEmpty(const Reader& reader, Packet& out) {
    if (!reader.atEnd()) return DecodeStatus::MalformedPacket;
    out = EmptyPacket{};
    return DecodeStatus::Ok;
}

// ---------------------------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------------------------

std::uint8_t headerByte(PacketType type, std::uint8_t flags = 0) noexcept {
    return static_cast<std::uint8_t>((static_cast<std::uint8_t>(type) << 4) | (flags & 0x0F));
}

/// Starts a packet: writes the fixed header and reserves room for the body.
std::vector<std::uint8_t> beginPacket(std::uint8_t header, std::size_t bodySize) {
    if (bodySize > kMaxRemainingLength) {
        throw std::length_error("MQTT packet body exceeds 268435455 bytes");
    }
    std::vector<std::uint8_t> out;
    out.reserve(1 + 4 + bodySize);
    out.push_back(header);
    encodeRemainingLength(static_cast<std::uint32_t>(bodySize), out);
    return out;
}

void putU16(std::vector<std::uint8_t>& out, std::uint16_t value) {
    out.push_back(static_cast<std::uint8_t>(value >> 8));
    out.push_back(static_cast<std::uint8_t>(value & 0xFF));
}

void putBytes(std::vector<std::uint8_t>& out, const void* data, std::size_t size) {
    if (size > 0xFFFF) {
        throw std::length_error("MQTT string or binary field exceeds 65535 bytes");
    }
    putU16(out, static_cast<std::uint16_t>(size));
    const auto* bytes = static_cast<const std::uint8_t*>(data);
    out.insert(out.end(), bytes, bytes + size);
}

void putString(std::vector<std::uint8_t>& out, std::string_view text) {
    putBytes(out, text.data(), text.size());
}

constexpr std::size_t fieldSize(std::size_t length) noexcept { return 2 + length; }

}  // namespace

// ---------------------------------------------------------------------------------------------
// Names
// ---------------------------------------------------------------------------------------------

const char* toString(PacketType type) noexcept {
    switch (type) {
        case PacketType::Connect: return "CONNECT";
        case PacketType::Connack: return "CONNACK";
        case PacketType::Publish: return "PUBLISH";
        case PacketType::Puback: return "PUBACK";
        case PacketType::Pubrec: return "PUBREC";
        case PacketType::Pubrel: return "PUBREL";
        case PacketType::Pubcomp: return "PUBCOMP";
        case PacketType::Subscribe: return "SUBSCRIBE";
        case PacketType::Suback: return "SUBACK";
        case PacketType::Unsubscribe: return "UNSUBSCRIBE";
        case PacketType::Unsuback: return "UNSUBACK";
        case PacketType::Pingreq: return "PINGREQ";
        case PacketType::Pingresp: return "PINGRESP";
        case PacketType::Disconnect: return "DISCONNECT";
    }
    return "UNKNOWN";
}

const char* toString(DecodeStatus status) noexcept {
    switch (status) {
        case DecodeStatus::Ok: return "ok";
        case DecodeStatus::NeedMoreData: return "need more data";
        case DecodeStatus::MalformedPacket: return "malformed packet";
        case DecodeStatus::ProtocolError: return "protocol error";
        case DecodeStatus::PacketTooLarge: return "packet too large";
    }
    return "unknown";
}

const char* toString(ConnectReturnCode code) noexcept {
    switch (code) {
        case ConnectReturnCode::Accepted: return "accepted";
        case ConnectReturnCode::UnacceptableProtocolVersion: return "unacceptable protocol version";
        case ConnectReturnCode::IdentifierRejected: return "identifier rejected";
        case ConnectReturnCode::ServerUnavailable: return "server unavailable";
        case ConnectReturnCode::BadUsernameOrPassword: return "bad username or password";
        case ConnectReturnCode::NotAuthorized: return "not authorized";
    }
    return "unknown";
}

PacketType packetTypeOf(const Packet& packet) noexcept {
    return std::visit(
        [](const auto& value) -> PacketType {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, ConnectPacket>) return PacketType::Connect;
            else if constexpr (std::is_same_v<T, ConnackPacket>) return PacketType::Connack;
            else if constexpr (std::is_same_v<T, PublishPacket>) return PacketType::Publish;
            else if constexpr (std::is_same_v<T, AckPacket>) return value.type;
            else if constexpr (std::is_same_v<T, SubscribePacket>) return PacketType::Subscribe;
            else if constexpr (std::is_same_v<T, SubackPacket>) return PacketType::Suback;
            else if constexpr (std::is_same_v<T, UnsubscribePacket>) return PacketType::Unsubscribe;
            else if constexpr (std::is_same_v<T, PingreqPacket>) return PacketType::Pingreq;
            else if constexpr (std::is_same_v<T, PingrespPacket>) return PacketType::Pingresp;
            else return PacketType::Disconnect;
        },
        packet);
}

// ---------------------------------------------------------------------------------------------
// Building blocks
// ---------------------------------------------------------------------------------------------

RemainingLength decodeRemainingLength(const std::uint8_t* data, std::size_t size) noexcept {
    std::uint32_t value = 0;
    std::uint32_t multiplier = 1;
    for (std::size_t i = 0; i < 4; ++i) {
        if (i >= size) {
            return {DecodeStatus::NeedMoreData, 0, 0};
        }
        const std::uint8_t byte = data[i];
        value += static_cast<std::uint32_t>(byte & 0x7F) * multiplier;
        if ((byte & 0x80) == 0) {
            return {DecodeStatus::Ok, value, i + 1};
        }
        multiplier *= 128;
    }
    return {DecodeStatus::MalformedPacket, 0, 0};  // A fifth continuation byte: MQTT-2.2.3
}

void encodeRemainingLength(std::uint32_t value, std::vector<std::uint8_t>& out) {
    if (value > kMaxRemainingLength) {
        throw std::length_error("remaining length out of range");
    }
    do {
        auto byte = static_cast<std::uint8_t>(value % 128);
        value /= 128;
        if (value > 0) byte |= 0x80;
        out.push_back(byte);
    } while (value > 0);
}

bool isValidMqttUtf8(std::string_view text) noexcept {
    std::size_t i = 0;
    const std::size_t size = text.size();
    while (i < size) {
        const auto lead = static_cast<std::uint8_t>(text[i]);
        if (lead == 0x00) return false;  // MQTT-1.5.3-2
        if (lead < 0x80) {
            ++i;
            continue;
        }

        std::size_t length = 0;
        std::uint32_t codePoint = 0;
        if ((lead & 0xE0) == 0xC0) {
            length = 2;
            codePoint = lead & 0x1Fu;
        } else if ((lead & 0xF0) == 0xE0) {
            length = 3;
            codePoint = lead & 0x0Fu;
        } else if ((lead & 0xF8) == 0xF0) {
            length = 4;
            codePoint = lead & 0x07u;
        } else {
            return false;
        }
        if (i + length > size) return false;
        for (std::size_t k = 1; k < length; ++k) {
            const auto continuation = static_cast<std::uint8_t>(text[i + k]);
            if ((continuation & 0xC0) != 0x80) return false;
            codePoint = (codePoint << 6) | (continuation & 0x3Fu);
        }
        const bool overlong = (length == 2 && codePoint < 0x80) ||
                              (length == 3 && codePoint < 0x800) ||
                              (length == 4 && codePoint < 0x10000);
        const bool surrogate = codePoint >= 0xD800 && codePoint <= 0xDFFF;  // MQTT-1.5.3-1
        if (overlong || surrogate || codePoint > 0x10FFFF) return false;
        i += length;
    }
    return true;
}

DecodeStatus decodePacket(std::uint8_t header, const std::uint8_t* body, std::size_t size, Role role,
                          Packet& out) {
    const auto typeValue = static_cast<std::uint8_t>(header >> 4);
    const auto flags = static_cast<std::uint8_t>(header & 0x0F);
    if (typeValue == 0 || typeValue == 15) {
        return DecodeStatus::MalformedPacket;  // Reserved packet types.
    }
    const auto type = static_cast<PacketType>(typeValue);
    if (!isAllowedFor(role, type)) {
        return DecodeStatus::ProtocolError;
    }
    if (type != PacketType::Publish && flags != requiredFlags(type)) {
        return DecodeStatus::MalformedPacket;  // MQTT-2.2.2-2
    }

    Reader reader(body, size);
    switch (type) {
        case PacketType::Connect: return decodeConnect(reader, out);
        case PacketType::Connack: return decodeConnack(reader, out);
        case PacketType::Publish: return decodePublish(flags, reader, out);
        case PacketType::Puback:
        case PacketType::Pubrec:
        case PacketType::Pubrel:
        case PacketType::Pubcomp:
        case PacketType::Unsuback: return decodeAck(type, reader, out);
        case PacketType::Subscribe: return decodeSubscribe(reader, out);
        case PacketType::Suback: return decodeSuback(reader, out);
        case PacketType::Unsubscribe: return decodeUnsubscribe(reader, out);
        case PacketType::Pingreq: return decodeEmpty<PingreqPacket>(reader, out);
        case PacketType::Pingresp: return decodeEmpty<PingrespPacket>(reader, out);
        case PacketType::Disconnect: return decodeEmpty<DisconnectPacket>(reader, out);
    }
    return DecodeStatus::MalformedPacket;
}

// ---------------------------------------------------------------------------------------------
// FrameDecoder
// ---------------------------------------------------------------------------------------------

void FrameDecoder::feed(const std::uint8_t* data, std::size_t size) {
    if (readOffset_ > 0) {
        // Drop already-decoded bytes before growing the buffer.
        buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(readOffset_));
        readOffset_ = 0;
    }
    buffer_.insert(buffer_.end(), data, data + size);
}

DecodeStatus FrameDecoder::next(Packet& out) {
    const std::uint8_t* data = buffer_.data() + readOffset_;
    const std::size_t available = bufferedBytes();
    if (available < 2) {
        return DecodeStatus::NeedMoreData;
    }

    const RemainingLength length = decodeRemainingLength(data + 1, available - 1);
    if (length.status != DecodeStatus::Ok) {
        return length.status;
    }
    // Reject oversized packets as soon as the header is known, before buffering the body.
    if (length.value > maxPacketSize_) {
        return DecodeStatus::PacketTooLarge;
    }
    const std::size_t total = 1 + length.bytesUsed + length.value;
    if (available < total) {
        return DecodeStatus::NeedMoreData;
    }

    const DecodeStatus status = decodePacket(data[0], data + 1 + length.bytesUsed, length.value, role_, out);
    readOffset_ += total;
    if (readOffset_ == buffer_.size()) {
        buffer_.clear();
        readOffset_ = 0;
    }
    return status;
}

// ---------------------------------------------------------------------------------------------
// Encoders
// ---------------------------------------------------------------------------------------------

std::vector<std::uint8_t> encodeConnect(const ConnectPacket& packet) {
    std::size_t bodySize = fieldSize(packet.protocolName.size()) + 1 + 1 + 2 +
                           fieldSize(packet.clientId.size());
    std::uint8_t flags = packet.cleanSession ? 0x02 : 0x00;
    if (packet.will) {
        flags |= static_cast<std::uint8_t>(0x04 | (static_cast<std::uint8_t>(packet.will->qos) << 3));
        if (packet.will->retain) flags |= 0x20;
        bodySize += fieldSize(packet.will->topic.size()) + fieldSize(packet.will->payload.size());
    }
    if (packet.username) {
        flags |= 0x80;
        bodySize += fieldSize(packet.username->size());
    }
    if (packet.password) {
        flags |= 0x40;
        bodySize += fieldSize(packet.password->size());
    }

    std::vector<std::uint8_t> out = beginPacket(headerByte(PacketType::Connect), bodySize);
    putString(out, packet.protocolName);
    out.push_back(packet.protocolLevel);
    out.push_back(flags);
    putU16(out, packet.keepAliveSeconds);
    putString(out, packet.clientId);
    if (packet.will) {
        putString(out, packet.will->topic);
        putBytes(out, packet.will->payload.data(), packet.will->payload.size());
    }
    if (packet.username) putString(out, *packet.username);
    if (packet.password) putBytes(out, packet.password->data(), packet.password->size());
    return out;
}

std::vector<std::uint8_t> encodeConnack(const ConnackPacket& packet) {
    std::vector<std::uint8_t> out = beginPacket(headerByte(PacketType::Connack), 2);
    out.push_back(packet.sessionPresent ? 0x01 : 0x00);
    out.push_back(static_cast<std::uint8_t>(packet.returnCode));
    return out;
}

std::vector<std::uint8_t> encodePublish(const PublishPacket& packet) {
    return encodePublish(packet.topic, packet.payload.data(), packet.payload.size(), packet.qos,
                         packet.retain, packet.dup, packet.packetId);
}

std::vector<std::uint8_t> encodePublish(std::string_view topic, const std::uint8_t* payload,
                                        std::size_t payloadSize, QoS qos, bool retain, bool dup,
                                        std::uint16_t packetId) {
    const bool hasPacketId = qos != QoS::AtMostOnce;
    const std::size_t bodySize = fieldSize(topic.size()) + (hasPacketId ? 2 : 0) + payloadSize;

    std::uint8_t flags = static_cast<std::uint8_t>(static_cast<std::uint8_t>(qos) << 1);
    if (dup) flags |= 0x08;
    if (retain) flags |= 0x01;

    std::vector<std::uint8_t> out = beginPacket(headerByte(PacketType::Publish, flags), bodySize);
    putString(out, topic);
    if (hasPacketId) putU16(out, packetId);
    out.insert(out.end(), payload, payload + payloadSize);
    return out;
}

std::vector<std::uint8_t> encodeAck(const AckPacket& packet) {
    std::vector<std::uint8_t> out = beginPacket(headerByte(packet.type, requiredFlags(packet.type)), 2);
    putU16(out, packet.packetId);
    return out;
}

std::vector<std::uint8_t> encodeSubscribe(const SubscribePacket& packet) {
    std::size_t bodySize = 2;
    for (const auto& request : packet.requests) bodySize += fieldSize(request.filter.size()) + 1;

    std::vector<std::uint8_t> out = beginPacket(headerByte(PacketType::Subscribe, 0x02), bodySize);
    putU16(out, packet.packetId);
    for (const auto& request : packet.requests) {
        putString(out, request.filter);
        out.push_back(static_cast<std::uint8_t>(request.qos));
    }
    return out;
}

std::vector<std::uint8_t> encodeSuback(const SubackPacket& packet) {
    std::vector<std::uint8_t> out =
        beginPacket(headerByte(PacketType::Suback), 2 + packet.returnCodes.size());
    putU16(out, packet.packetId);
    out.insert(out.end(), packet.returnCodes.begin(), packet.returnCodes.end());
    return out;
}

std::vector<std::uint8_t> encodeUnsubscribe(const UnsubscribePacket& packet) {
    std::size_t bodySize = 2;
    for (const auto& filter : packet.filters) bodySize += fieldSize(filter.size());

    std::vector<std::uint8_t> out = beginPacket(headerByte(PacketType::Unsubscribe, 0x02), bodySize);
    putU16(out, packet.packetId);
    for (const auto& filter : packet.filters) putString(out, filter);
    return out;
}

std::vector<std::uint8_t> encodePingreq() { return beginPacket(headerByte(PacketType::Pingreq), 0); }
std::vector<std::uint8_t> encodePingresp() { return beginPacket(headerByte(PacketType::Pingresp), 0); }
std::vector<std::uint8_t> encodeDisconnect() {
    return beginPacket(headerByte(PacketType::Disconnect), 0);
}

std::vector<std::uint8_t> encode(const Packet& packet) {
    return std::visit(
        [](const auto& value) -> std::vector<std::uint8_t> {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, ConnectPacket>) return encodeConnect(value);
            else if constexpr (std::is_same_v<T, ConnackPacket>) return encodeConnack(value);
            else if constexpr (std::is_same_v<T, PublishPacket>) return encodePublish(value);
            else if constexpr (std::is_same_v<T, AckPacket>) return encodeAck(value);
            else if constexpr (std::is_same_v<T, SubscribePacket>) return encodeSubscribe(value);
            else if constexpr (std::is_same_v<T, SubackPacket>) return encodeSuback(value);
            else if constexpr (std::is_same_v<T, UnsubscribePacket>) return encodeUnsubscribe(value);
            else if constexpr (std::is_same_v<T, PingreqPacket>) return encodePingreq();
            else if constexpr (std::is_same_v<T, PingrespPacket>) return encodePingresp();
            else return encodeDisconnect();
        },
        packet);
}

}  // namespace pocketmqtt::mqtt
