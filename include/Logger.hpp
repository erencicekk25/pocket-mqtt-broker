#pragma once

#include <atomic>
#include <mutex>
#include <sstream>
#include <string_view>
#include <utility>

namespace pocketmqtt {

enum class LogLevel : int { Debug = 0, Info = 1, Warn = 2, Error = 3 };

/// Process-wide, thread-safe logger that writes single, optionally colored lines to stdout.
///
/// Colors are enabled automatically when stdout is an interactive terminal and the
/// `NO_COLOR` environment variable (https://no-color.org) is not set.
class Logger {
public:
    static Logger& instance() noexcept;

    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    void setLevel(LogLevel level) noexcept { level_.store(level, std::memory_order_relaxed); }
    [[nodiscard]] LogLevel level() const noexcept { return level_.load(std::memory_order_relaxed); }
    [[nodiscard]] bool enabled(LogLevel level) const noexcept { return level >= this->level(); }

    void setColorEnabled(bool enabled) noexcept { color_.store(enabled, std::memory_order_relaxed); }
    [[nodiscard]] bool colorEnabled() const noexcept { return color_.load(std::memory_order_relaxed); }

    void write(LogLevel level, std::string_view component, std::string_view message);

private:
    Logger();

    std::atomic<LogLevel> level_{LogLevel::Info};
    std::atomic<bool> color_{false};
    std::mutex mutex_;
};

/// Formats all arguments with `operator<<` and logs them as one line.
/// Arguments are not evaluated into a string at all when the level is disabled.
template <typename... Args>
void log(LogLevel level, std::string_view component, Args&&... args) {
    Logger& logger = Logger::instance();
    if (!logger.enabled(level)) {
        return;
    }
    std::ostringstream stream;
    (stream << ... << std::forward<Args>(args));
    logger.write(level, component, stream.str());
}

template <typename... Args>
void logDebug(std::string_view component, Args&&... args) {
    log(LogLevel::Debug, component, std::forward<Args>(args)...);
}

template <typename... Args>
void logInfo(std::string_view component, Args&&... args) {
    log(LogLevel::Info, component, std::forward<Args>(args)...);
}

template <typename... Args>
void logWarn(std::string_view component, Args&&... args) {
    log(LogLevel::Warn, component, std::forward<Args>(args)...);
}

template <typename... Args>
void logError(std::string_view component, Args&&... args) {
    log(LogLevel::Error, component, std::forward<Args>(args)...);
}

}  // namespace pocketmqtt
