#pragma once

// Minimal self-registering test harness, so the project stays free of external dependencies.

#include <cstdio>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace pmtest {

struct TestCase {
    const char* name;
    void (*function)();
};

inline std::vector<TestCase>& registry() {
    static std::vector<TestCase> tests;
    return tests;
}

inline int& currentFailures() {
    static int failures = 0;
    return failures;
}

struct Registrar {
    Registrar(const char* name, void (*function)()) { registry().push_back({name, function}); }
};

template <typename T, typename = void>
struct IsStreamable : std::false_type {};
template <typename T>
struct IsStreamable<T, std::void_t<decltype(std::declval<std::ostream&>() << std::declval<const T&>())>>
    : std::true_type {};

template <typename T>
std::string describe(const T& value) {
    std::ostringstream stream;
    if constexpr (std::is_enum_v<T>) {
        stream << static_cast<long long>(value);
    } else if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
        stream << static_cast<long long>(value);
    } else if constexpr (IsStreamable<T>::value) {
        stream << std::boolalpha << value;
    } else {
        stream << "<unprintable>";
    }
    return stream.str();
}

inline void fail(const char* file, int line, const std::string& message) {
    std::printf("    FAIL %s:%d\n         %s\n", file, line, message.c_str());
    ++currentFailures();
}

inline int runAll() {
    int failedTests = 0;
    for (const TestCase& test : registry()) {
        currentFailures() = 0;
        test.function();
        const bool passed = currentFailures() == 0;
        std::printf("[%s] %s\n", passed ? " ok " : "FAIL", test.name);
        if (!passed) ++failedTests;
    }
    std::printf("\n%zu tests, %d failed\n", registry().size(), failedTests);
    return failedTests == 0 ? 0 : 1;
}

}  // namespace pmtest

#define PM_CONCAT_INNER(a, b) a##b
#define PM_CONCAT(a, b) PM_CONCAT_INNER(a, b)

#define TEST_CASE(name)                                                       \
    static void name();                                                       \
    static const ::pmtest::Registrar PM_CONCAT(name, _registrar)(#name, &name); \
    static void name()

#define CHECK(expression)                                                    \
    do {                                                                     \
        if (!(expression)) ::pmtest::fail(__FILE__, __LINE__, "CHECK(" #expression ")"); \
    } while (0)

#define CHECK_EQ(actual, expected)                                                         \
    do {                                                                                   \
        const auto& pmActual = (actual);                                                   \
        const auto& pmExpected = (expected);                                               \
        if (!(pmActual == pmExpected)) {                                                   \
            ::pmtest::fail(__FILE__, __LINE__,                                             \
                           "CHECK_EQ(" #actual ", " #expected "): got " +                   \
                               ::pmtest::describe(pmActual) + ", expected " +              \
                               ::pmtest::describe(pmExpected));                            \
        }                                                                                  \
    } while (0)

/// Like CHECK, but aborts the current test case on failure.
#define REQUIRE(expression)                                                         \
    do {                                                                            \
        if (!(expression)) {                                                        \
            ::pmtest::fail(__FILE__, __LINE__, "REQUIRE(" #expression ")");         \
            return;                                                                 \
        }                                                                           \
    } while (0)
