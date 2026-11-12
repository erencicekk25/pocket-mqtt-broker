#include "Logger.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <string>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <io.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace pocketmqtt {
namespace {

constexpr std::string_view kReset = "\x1b[0m";
constexpr std::string_view kDim = "\x1b[2m";
constexpr std::string_view kCyan = "\x1b[36m";

std::string_view levelLabel(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info: return "INFO ";
        case LogLevel::Warn: return "WARN ";
        case LogLevel::Error: return "ERROR";
    }
    return "?????";
}

std::string_view levelColor(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::Debug: return "\x1b[90m";
        case LogLevel::Info: return "\x1b[32m";
        case LogLevel::Warn: return "\x1b[33m";
        case LogLevel::Error: return "\x1b[1;31m";
    }
    return kReset;
}

bool stdoutIsTerminal() noexcept {
#ifdef _WIN32
    return _isatty(_fileno(stdout)) != 0;
#else
    return isatty(fileno(stdout)) != 0;
#endif
}

bool detectColorSupport() noexcept {
    if (std::getenv("NO_COLOR") != nullptr || !stdoutIsTerminal()) {
        return false;
    }
#ifdef _WIN32
    // Modern Windows consoles understand ANSI escapes once VT processing is switched on.
    HANDLE console = GetStdHandle(STD_OUTPUT_HANDLE);
    DWORD mode = 0;
    if (console == INVALID_HANDLE_VALUE || GetConsoleMode(console, &mode) == 0) {
        return false;
    }
    return SetConsoleMode(console, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING) != 0;
#else
    return true;
#endif
}

std::tm toLocalTime(std::time_t time) noexcept {
    std::tm result{};
#ifdef _WIN32
    localtime_s(&result, &time);
#else
    localtime_r(&time, &result);
#endif
    return result;
}

}  // namespace

Logger& Logger::instance() noexcept {
    static Logger logger;
    return logger;
}

Logger::Logger() : color_(detectColorSupport()) {}

void Logger::write(LogLevel level, std::string_view component, std::string_view message) {
    using std::chrono::system_clock;
    const auto now = system_clock::now();
    const auto millis =
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;
    const std::tm local = toLocalTime(system_clock::to_time_t(now));

    char timestamp[32];
    const std::size_t length = std::strftime(timestamp, sizeof(timestamp), "%H:%M:%S", &local);
    std::snprintf(timestamp + length, sizeof(timestamp) - length, ".%03d", static_cast<int>(millis));

    constexpr std::size_t kComponentWidth = 7;
    std::string paddedComponent(component);
    if (paddedComponent.size() < kComponentWidth) {
        paddedComponent.append(kComponentWidth - paddedComponent.size(), ' ');
    }

    const bool color = colorEnabled();
    std::string line;
    line.reserve(message.size() + 64);
    if (color) line += kDim;
    line += timestamp;
    if (color) line += kReset;
    line += ' ';
    if (color) line += levelColor(level);
    line += levelLabel(level);
    if (color) line += kReset;
    line += ' ';
    if (color) line += kCyan;
    line += paddedComponent;
    if (color) line += kReset;
    line += ' ';
    line += message;
    line += '\n';

    const std::lock_guard<std::mutex> lock(mutex_);
    std::fwrite(line.data(), 1, line.size(), stdout);
    std::fflush(stdout);
}

}  // namespace pocketmqtt
