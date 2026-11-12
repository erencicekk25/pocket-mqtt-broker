#include "Logger.hpp"
#include "TestFramework.hpp"

int main() {
    // Keep test output readable; the broker logs every connection at INFO level.
    pocketmqtt::Logger::instance().setLevel(pocketmqtt::LogLevel::Error);
    return pmtest::runAll();
}
