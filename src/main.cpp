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
    LogLevel logLevel = LogLevel::Info;
    bool color = true;
    bool showHelp = false;
    bool showVersion = false;
};

void printUsage() {
    std::printf(
        "Usage: pocket-mqtt [options]\n"
        "\n"
        "Options:\n"
        "  -b, --bind <address>        IPv4 address to listen on       (default: 0.0.0.0)\n"
        "  -p, --port <port>           TCP port to listen on            (default: 1883)\n"
        "      --max-connections <n>   Maximum concurrent connections   (default: 10000)\n"
        "  -v, --verbose               Enable debug logging\n"
        "      --no-color              Disable colored output\n"
        "  -h, --help                  Show this help and exit\n"
        "      --version               Show version and exit\n");
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
        const auto nextValue = [&](std::string_view& value) {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "error: %s requires a value\n", argv[i]);
                return false;
            }
            value = argv[++i];
            return true;
        };

        std::string_view value;
        unsigned long long number = 0;
        if (arg == "-h" || arg == "--help") {
            options.showHelp = true;
        } else if (arg == "--version") {
            options.showVersion = true;
        } else if (arg == "-v" || arg == "--verbose") {
            options.logLevel = LogLevel::Debug;
        } else if (arg == "--no-color") {
            options.color = false;
        } else if (arg == "-b" || arg == "--bind") {
            if (!nextValue(value)) return std::nullopt;
            options.tcp.bindAddress = std::string(value);
        } else if (arg == "-p" || arg == "--port") {
            if (!nextValue(value)) return std::nullopt;
            if (!parseUnsigned(value, 65535, number)) {
                std::fprintf(stderr, "error: invalid port '%s'\n", std::string(value).c_str());
                return std::nullopt;
            }
            options.tcp.port = static_cast<std::uint16_t>(number);
        } else if (arg == "--max-connections") {
            if (!nextValue(value)) return std::nullopt;
            if (!parseUnsigned(value, 1'000'000, number) || number == 0) {
                std::fprintf(stderr, "error: invalid connection limit '%s'\n",
                             std::string(value).c_str());
                return std::nullopt;
            }
            options.tcp.maxConnections = static_cast<std::size_t>(number);
        } else {
            std::fprintf(stderr, "error: unknown option '%s' (see --help)\n",
                         std::string(arg).c_str());
            return std::nullopt;
        }
    }
    return options;
}

/// Temporary handler for the networking milestone: logs connection lifecycle and traffic.
class ConnectionLogger final : public IConnectionHandler {
public:
    void onAccept(ConnectionId id, const std::string& peer) override {
        logInfo("server", "connection #", id, " opened from ", peer);
    }
    void onData(ConnectionId id, const std::uint8_t*, std::size_t size) override {
        logDebug("server", "connection #", id, " received ", size, " bytes");
    }
    void onClose(ConnectionId id) override { logInfo("server", "connection #", id, " closed"); }
    void onTick(Clock::time_point) override {}
};

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

    try {
        TcpServer server(options->tcp);
        server.listen();
        logInfo("server", "pocket-mqtt ", POCKET_MQTT_VERSION, " listening on ",
                options->tcp.bindAddress, ':', server.port());

        g_server.store(&server);
        std::signal(SIGINT, handleSignal);
        std::signal(SIGTERM, handleSignal);

        ConnectionLogger handler;
        server.run(handler);

        g_server.store(nullptr);
        logInfo("server", "shut down cleanly");
    } catch (const std::exception& error) {
        logError("server", "fatal: ", error.what());
        return 1;
    }
    return 0;
}
