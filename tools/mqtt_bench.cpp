// mqtt-bench: a small, dependency-free load generator for MQTT 3.1.1 brokers.
//
//   throughput mode: P publishers flood "bench/<n>/data", S subscribers on "bench/+/data"
//                    receive everything; reports messages/s in and out of the broker.
//   latency mode:    one publisher, one subscriber, strictly one message in flight;
//                    reports the publish -> deliver round trip through the broker.

#include "MqttProtocol.hpp"
#include "TcpServer.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

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
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#endif

namespace {

using namespace pocketmqtt;
using namespace pocketmqtt::mqtt;
using BenchClock = std::chrono::steady_clock;

struct BenchOptions {
    std::string host = "127.0.0.1";
    std::uint16_t port = 1883;
    std::string mode = "throughput";
    int publishers = 4;
    int subscribers = 4;
    int messages = 50'000;  // Per publisher.
    std::size_t payloadSize = 64;
    QoS qos = QoS::AtMostOnce;
};

std::int64_t nowNanos() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(BenchClock::now().time_since_epoch())
        .count();
}

/// Blocking MQTT client over one TCP connection, just enough for benchmarking.
class BenchClient {
public:
    BenchClient(const BenchOptions& options, const std::string& clientId) : buffer_(64 * 1024) {
        socket_ = Socket(static_cast<NativeSocket>(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)));
        if (!socket_.valid()) throw std::runtime_error("socket() failed");

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(options.port);
        if (::inet_pton(AF_INET, options.host.c_str(), &address.sin_addr) != 1) {
            throw std::runtime_error("invalid IPv4 host: " + options.host);
        }
        if (::connect(native(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
            throw std::runtime_error("cannot connect to " + options.host + ':' +
                                     std::to_string(options.port));
        }
        const int one = 1;
        ::setsockopt(native(), IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one), sizeof(one));
#ifdef _WIN32
        const DWORD timeoutMs = 10'000;
        ::setsockopt(native(), SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeoutMs),
                     sizeof(timeoutMs));
#else
        timeval timeout{10, 0};
        ::setsockopt(native(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
#endif

        ConnectPacket connect;
        connect.clientId = clientId;
        connect.keepAliveSeconds = 0;
        sendAll(encodeConnect(connect));
        const Packet reply = receive();
        const auto* connack = std::get_if<ConnackPacket>(&reply);
        if (connack == nullptr || connack->returnCode != ConnectReturnCode::Accepted) {
            throw std::runtime_error("broker refused CONNECT for " + clientId);
        }
    }

    void subscribe(const std::string& filter, QoS qos) {
        sendAll(encodeSubscribe({1, {{filter, qos}}}));
        const Packet reply = receive();
        if (!std::holds_alternative<SubackPacket>(reply)) {
            throw std::runtime_error("expected SUBACK");
        }
    }

    void sendAll(const std::vector<std::uint8_t>& bytes) {
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            const auto sent = ::send(native(), reinterpret_cast<const char*>(bytes.data() + offset),
                                     static_cast<int>(bytes.size() - offset), 0);
            if (sent <= 0) throw std::runtime_error("send failed");
            offset += static_cast<std::size_t>(sent);
        }
    }

    /// Buffers bytes (e.g. PUBACKs) that are flushed before the next blocking read.
    void queue(const std::vector<std::uint8_t>& bytes) {
        pending_.insert(pending_.end(), bytes.begin(), bytes.end());
    }

    Packet receive() {
        Packet packet;
        for (;;) {
            const DecodeStatus status = decoder_.next(packet);
            if (status == DecodeStatus::Ok) return packet;
            if (status != DecodeStatus::NeedMoreData) {
                throw std::runtime_error(std::string("decode error: ") + toString(status));
            }
            if (!pending_.empty()) {
                sendAll(pending_);
                pending_.clear();
            }
            const auto received = ::recv(native(), reinterpret_cast<char*>(buffer_.data()),
                                         static_cast<int>(buffer_.size()), 0);
            if (received <= 0) throw std::runtime_error("connection closed or timed out");
            decoder_.feed(buffer_.data(), static_cast<std::size_t>(received));
        }
    }

    void disconnect() { sendAll(encodeDisconnect()); }

private:
#ifdef _WIN32
    SOCKET native() const { return static_cast<SOCKET>(socket_.native()); }
#else
    int native() const { return socket_.native(); }
#endif

    Socket socket_;
    FrameDecoder decoder_{Role::Client, 1u << 26};
    std::vector<std::uint8_t> buffer_;
    std::vector<std::uint8_t> pending_;
};

PublishPacket makeMessage(const std::string& topic, std::size_t payloadSize, QoS qos) {
    PublishPacket packet;
    packet.topic = topic;
    packet.payload.assign(std::max<std::size_t>(payloadSize, sizeof(std::int64_t)), 0xAB);
    packet.qos = qos;
    return packet;
}

void stampNow(PublishPacket& packet) {
    const std::int64_t now = nowNanos();
    std::memcpy(packet.payload.data(), &now, sizeof(now));
}

std::int64_t stampOf(const PublishPacket& packet) {
    std::int64_t stamp = 0;
    std::memcpy(&stamp, packet.payload.data(), sizeof(stamp));
    return stamp;
}

double percentile(std::vector<double>& samples, double p) {
    if (samples.empty()) return 0.0;
    const auto index = static_cast<std::size_t>(p * static_cast<double>(samples.size() - 1));
    std::nth_element(samples.begin(), samples.begin() + static_cast<std::ptrdiff_t>(index), samples.end());
    return samples[index];
}

int runThroughput(const BenchOptions& options) {
    const long long expectedPerSubscriber =
        static_cast<long long>(options.publishers) * options.messages;

    std::vector<BenchClient> subscribers;
    subscribers.reserve(static_cast<std::size_t>(options.subscribers));
    for (int i = 0; i < options.subscribers; ++i) {
        subscribers.emplace_back(options, "bench-sub-" + std::to_string(i));
        subscribers.back().subscribe("bench/+/data", options.qos);
    }
    std::vector<BenchClient> publishers;
    publishers.reserve(static_cast<std::size_t>(options.publishers));
    for (int i = 0; i < options.publishers; ++i) {
        publishers.emplace_back(options, "bench-pub-" + std::to_string(i));
    }

    std::atomic<bool> start{false};
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;

    for (auto& subscriber : subscribers) {
        threads.emplace_back([&, client = &subscriber] {
            long long received = 0;
            try {
                while (received < expectedPerSubscriber) {
                    Packet packet = client->receive();
                    if (auto* publish = std::get_if<PublishPacket>(&packet)) {
                        ++received;
                        if (publish->qos == QoS::AtLeastOnce) {
                            client->queue(encodeAck({PacketType::Puback, publish->packetId}));
                        }
                    }
                }
            } catch (const std::exception& error) {
                std::fprintf(stderr,
                             "subscriber stopped after %lld of %lld messages: %s\n"
                             "  (QoS 1 floods can exceed the broker's per-session queue; "
                             "try pocket-mqtt --max-queued 1000000)\n",
                             received, expectedPerSubscriber, error.what());
                ++failures;
            }
        });
    }

    for (int p = 0; p < options.publishers; ++p) {
        threads.emplace_back([&, p, client = &publishers[static_cast<std::size_t>(p)]] {
            try {
                PublishPacket message =
                    makeMessage("bench/" + std::to_string(p) + "/data", options.payloadSize, options.qos);
                constexpr int kBatch = 32;  // Packets coalesced per send() call.
                constexpr int kWindow = 64;  // Unacknowledged QoS 1 publishes allowed.
                const bool acknowledged = options.qos == QoS::AtLeastOnce;
                int inflight = 0;
                int batched = 0;
                std::vector<std::uint8_t> batch;
                while (!start.load()) std::this_thread::yield();

                for (int i = 0; i < options.messages; ++i) {
                    if (acknowledged) message.packetId = static_cast<std::uint16_t>(i % 60'000 + 1);
                    stampNow(message);
                    const std::vector<std::uint8_t> bytes = encodePublish(message);
                    batch.insert(batch.end(), bytes.begin(), bytes.end());
                    if (++batched < kBatch && i + 1 < options.messages) continue;

                    client->sendAll(batch);
                    batch.clear();
                    if (acknowledged) {
                        inflight += batched;
                        while (inflight > kWindow) {
                            if (std::holds_alternative<AckPacket>(client->receive())) --inflight;
                        }
                    }
                    batched = 0;
                }
                while (options.qos == QoS::AtLeastOnce && inflight > 0) {
                    if (std::holds_alternative<AckPacket>(client->receive())) --inflight;
                }
            } catch (const std::exception& error) {
                std::fprintf(stderr, "publisher failed: %s\n", error.what());
                ++failures;
            }
        });
    }

    const auto begin = BenchClock::now();
    start.store(true);
    for (auto& thread : threads) thread.join();
    const double seconds = std::chrono::duration<double>(BenchClock::now() - begin).count();

    for (auto& client : publishers) client.disconnect();
    for (auto& client : subscribers) client.disconnect();
    if (failures.load() != 0) return 1;

    const double published = static_cast<double>(expectedPerSubscriber);
    const double delivered = published * options.subscribers;
    std::printf("mode         throughput (QoS %d, %zu-byte payload)\n", static_cast<int>(options.qos),
                options.payloadSize);
    std::printf("clients      %d publishers -> %d subscribers\n", options.publishers,
                options.subscribers);
    std::printf("elapsed      %.3f s\n", seconds);
    std::printf("published    %.0f msgs  (%.0f msg/s in)\n", published, published / seconds);
    std::printf("delivered    %.0f msgs  (%.0f msg/s out)\n", delivered, delivered / seconds);
    return 0;
}

int runLatency(const BenchOptions& options) {
    BenchClient subscriber(options, "bench-lat-sub");
    subscriber.subscribe("bench/latency", options.qos);
    BenchClient publisher(options, "bench-lat-pub");
    PublishPacket message = makeMessage("bench/latency", options.payloadSize, options.qos);

    std::vector<double> samples;
    samples.reserve(static_cast<std::size_t>(options.messages));
    for (int i = 0; i < options.messages; ++i) {
        if (options.qos == QoS::AtLeastOnce) message.packetId = static_cast<std::uint16_t>(i % 60'000 + 1);
        stampNow(message);
        publisher.sendAll(encodePublish(message));
        for (;;) {
            Packet packet = subscriber.receive();
            if (auto* publish = std::get_if<PublishPacket>(&packet)) {
                samples.push_back(static_cast<double>(nowNanos() - stampOf(*publish)) / 1000.0);
                if (publish->qos == QoS::AtLeastOnce) {
                    subscriber.sendAll(encodeAck({PacketType::Puback, publish->packetId}));
                }
                break;
            }
        }
        if (options.qos == QoS::AtLeastOnce) publisher.receive();  // PUBACK
    }
    publisher.disconnect();
    subscriber.disconnect();

    std::printf("mode         latency (QoS %d, %zu-byte payload, %d samples)\n",
                static_cast<int>(options.qos), options.payloadSize, options.messages);
    std::printf("p50          %.1f us\n", percentile(samples, 0.50));
    std::printf("p99          %.1f us\n", percentile(samples, 0.99));
    std::printf("max          %.1f us\n", percentile(samples, 1.0));
    return 0;
}

void printUsage() {
    std::printf(
        "Usage: mqtt-bench [options]\n"
        "  --host <ipv4>          Broker address           (default: 127.0.0.1)\n"
        "  --port <port>          Broker port              (default: 1883)\n"
        "  --mode <name>          throughput | latency     (default: throughput)\n"
        "  --publishers <n>       Publisher connections    (default: 4)\n"
        "  --subscribers <n>      Subscriber connections   (default: 4)\n"
        "  --messages <n>         Messages per publisher   (default: 50000)\n"
        "  --payload <bytes>      Payload size, min 8      (default: 64)\n"
        "  --qos <0|1>            Publish/subscribe QoS    (default: 0)\n");
}

}  // namespace

int main(int argc, char** argv) {
    BenchOptions options;
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg = argv[i];
            if (arg == "-h" || arg == "--help") {
                printUsage();
                return 0;
            }
            if (i + 1 >= argc) throw std::invalid_argument("missing value for " + std::string(arg));
            const std::string value = argv[++i];
            if (arg == "--host") options.host = value;
            else if (arg == "--port") options.port = static_cast<std::uint16_t>(std::stoi(value));
            else if (arg == "--mode") options.mode = value;
            else if (arg == "--publishers") options.publishers = std::stoi(value);
            else if (arg == "--subscribers") options.subscribers = std::stoi(value);
            else if (arg == "--messages") options.messages = std::stoi(value);
            else if (arg == "--payload") options.payloadSize = static_cast<std::size_t>(std::stoul(value));
            else if (arg == "--qos") options.qos = std::stoi(value) == 1 ? QoS::AtLeastOnce : QoS::AtMostOnce;
            else throw std::invalid_argument("unknown option " + std::string(arg));
        }
        if (options.publishers < 1 || options.subscribers < 1 || options.messages < 1) {
            throw std::invalid_argument("publishers, subscribers and messages must be positive");
        }

        NetworkRuntime runtime;
        if (options.mode == "throughput") return runThroughput(options);
        if (options.mode == "latency") return runLatency(options);
        throw std::invalid_argument("unknown mode " + options.mode);
    } catch (const std::exception& error) {
        std::fprintf(stderr, "mqtt-bench: %s\n", error.what());
        return 1;
    }
}
