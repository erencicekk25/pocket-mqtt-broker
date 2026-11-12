#include "BrokerEngine.hpp"
#include "Logger.hpp"
#include "TcpServer.hpp"

#include <atomic>
#include <csignal>
#include <cstdio>
#include <exception>
#include <optional>
#include <string>
#include <string_view>

#ifndef POCKET_MQTT_VERSION
#define POCKET_MQTT_VERSION "dev"
#endif

namespace {

using namespace pocketmqtt;

std::atomic<TcpServer*> g_server{nullptr};

extern "C" void handleSignal(int) {
    if (TcpServer* server = g_server.load()) {
        server->stop();
    }
}

struct Options {
    TcpServerConfig tcp;
    BrokerConfig broker;
    LogLevel logLevel = LogLevel::Info;
    bool color = true;
    bool showHelp = false;
    bool showVersion = false;
};

void printUsage() {
    std::printf(
        "pocket-mqtt %s - a zero-dependency MQTT 3.1.1 broker\n"
        "\n"
        "Usage: pocket-mqtt [options]\n"
        "\n"
        "Options:\n"
        "  -b, --bind <address>        IPv4 address to listen on        (default: 0.0.0.0)\n"
        "  -p, --port <port>           TCP port, 0 picks a free one      (default: 1883)\n"
        "      --max-connections <n>   Concurrent connection limit       (default: 10000)\n"
        "      --max-packet-size <n>   Largest accepted packet in bytes  (default: 1048576)\n"
        "      --max-queued <n>        QoS 1 messages queued per session (default: 1000)\n"
        "      --max-inflight <n>      Unacked QoS 1 messages per client (default: 32)\n"
        "      --stats <seconds>       Log broker statistics periodically (default: off)\n"
        "  -v, --verbose               Debug logging, including every PUBLISH\n"
        "  -q, --quiet                 Only log warnings and errors\n"
        "      --no-color              Disable colored output (NO_COLOR is honored too)\n"
        "  -h, --help                  Show this help and exit\n"
        "      --version               Show version and exit\n",
        POCKET_MQTT_VERSION);
}

bool parseUnsigned(std::string_view text, unsigned long long max, unsigned long long& out) {
    if (text.empty()) return false;
    unsigned long long value = 0;
    for (const char c : text) {
        if (c < '0' || c > '9') return false;
        value = value * 10 + static_cast<unsigned long long>(c - '0');
        if (value > max) return false;
    }
    out = value;
    return true;
}

std::optional<Options> parseArguments(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];

        // Fetches and validates the numeric value that follows the current flag.
        const auto numberArgument = [&](unsigned long long min, unsigned long long max,
                                        unsigned long long& out) {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "error: %s requires a value\n", argv[i]);
                return false;
            }
            const char* value = argv[++i];
            if (!parseUnsigned(value, max, out) || out < min) {
                std::fprintf(stderr, "error: invalid value '%s' for %s (expected %llu-%llu)\n",
                             value, argv[i - 1], min, max);
                return false;
            }
            return true;
        };

        unsigned long long number = 0;
        if (arg == "-h" || arg == "--help") {
            options.showHelp = true;
        } else if (arg == "--version") {
            options.showVersion = true;
        } else if (arg == "-v" || arg == "--verbose") {
            options.logLevel = LogLevel::Debug;
        } else if (arg == "-q" || arg == "--quiet") {
            options.logLevel = LogLevel::Warn;
        } else if (arg == "--no-color") {
            options.color = false;
        } else if (arg == "-b" || arg == "--bind") {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "error: %s requires a value\n", argv[i]);
                return std::nullopt;
            }
            options.tcp.bindAddress = argv[++i];
        } else if (arg == "-p" || arg == "--port") {
            if (!numberArgument(0, 65535, number)) return std::nullopt;
            options.tcp.port = static_cast<std::uint16_t>(number);
        } else if (arg == "--max-connections") {
            if (!numberArgument(1, 1'000'000, number)) return std::nullopt;
            options.tcp.maxConnections = static_cast<std::size_t>(number);
        } else if (arg == "--max-packet-size") {
            if (!numberArgument(64, mqtt::kMaxRemainingLength, number)) return std::nullopt;
            options.broker.maxPacketSize = static_cast<std::size_t>(number);
        } else if (arg == "--max-queued") {
            if (!numberArgument(0, 10'000'000, number)) return std::nullopt;
            options.broker.maxQueuedMessages = static_cast<std::size_t>(number);
        } else if (arg == "--max-inflight") {
            if (!numberArgument(1, 65'000, number)) return std::nullopt;
            options.broker.maxInflightMessages = static_cast<std::size_t>(number);
        } else if (arg == "--stats") {
            if (!numberArgument(0, 86'400, number)) return std::nullopt;
            options.broker.statsInterval = std::chrono::seconds(number);
        } else {
            std::fprintf(stderr, "error: unknown option '%s' (see --help)\n", argv[i]);
            return std::nullopt;
        }
    }
    return options;
}

void printBanner(bool color) {
    const char* accent = color ? "\x1b[38;5;45m" : "";
    const char* dim = color ? "\x1b[2m" : "";
    const char* reset = color ? "\x1b[0m" : "";
    std::printf(
        "%s"
        "                 _        _                       _   _   \n"
        "  _ __  ___  ___| | _____| |_     _ __ ___   __ _| |_| |_ \n"
        " | '_ \\/ _ \\/ __| |/ / _ \\ __|___| '_ ` _ \\ / _` | __| __|\n"
        " | |_) | (_) | (__|   <  __/ ||___| | | | | | (_| | |_| |_ \n"
        " | .__/ \\___/ \\___|_|\\_\\___|\\__|  |_| |_| |_|\\__, |\\__|\\__|\n"
        " |_|                                            |_|        \n"
        "%s  v%s  -  zero-dependency MQTT 3.1.1 broker%s\n\n",
        accent, dim, POCKET_MQTT_VERSION, reset);
    std::fflush(stdout);
}

}  // namespace

int main(int argc, char** argv) {
    const std::optional<Options> options = parseArguments(argc, argv);
    if (!options) {
        return 2;
    }
    if (options->showHelp) {
        printUsage();
        return 0;
    }
    if (options->showVersion) {
        std::printf("pocket-mqtt %s\n", POCKET_MQTT_VERSION);
        return 0;
    }

    Logger& logger = Logger::instance();
    logger.setLevel(options->logLevel);
    if (!options->color) {
        logger.setColorEnabled(false);
    }
    if (options->logLevel != LogLevel::Warn) {
        printBanner(logger.colorEnabled());
    }

    try {
        TcpServer server(options->tcp);
        server.listen();
        BrokerEngine broker(server, options->broker);

        g_server.store(&server);
        std::signal(SIGINT, handleSignal);
        std::signal(SIGTERM, handleSignal);

        logInfo("server", "listening on mqtt://", options->tcp.bindAddress, ':', server.port(),
                "  (Ctrl+C to stop)");
        server.run(broker);
        g_server.store(nullptr);

        const BrokerStats stats = broker.stats();
        logInfo("server", "shut down cleanly after ", stats.connectionsAccepted, " connections, ",
                stats.messagesReceived, " messages in, ", stats.messagesDelivered, " out");
    } catch (const std::exception& error) {
        g_server.store(nullptr);
        logError("server", "fatal: ", error.what());
        return 1;
    }
    return 0;
}
