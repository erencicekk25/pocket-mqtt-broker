cmake_minimum_required(VERSION 3.16)

project(pocket_mqtt_broker
    VERSION 0.1.0
    DESCRIPTION "Zero-dependency MQTT 3.1.1 broker in modern C++17"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(POCKET_MQTT_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)
option(POCKET_MQTT_SANITIZE "Build with AddressSanitizer + UndefinedBehaviorSanitizer (GCC/Clang)" OFF)
option(POCKET_MQTT_BUILD_TOOLS "Build the mqtt-bench load generator" ON)

# Shared warning set, applied to every target we own.
add_library(pocket_mqtt_warnings INTERFACE)
if(MSVC)
    target_compile_options(pocket_mqtt_warnings INTERFACE /W4 /permissive-)
    if(POCKET_MQTT_WARNINGS_AS_ERRORS)
        target_compile_options(pocket_mqtt_warnings INTERFACE /WX)
    endif()
else()
    target_compile_options(pocket_mqtt_warnings INTERFACE -Wall -Wextra -Wpedantic -Wshadow)
    if(POCKET_MQTT_WARNINGS_AS_ERRORS)
        target_compile_options(pocket_mqtt_warnings INTERFACE -Werror)
    endif()
endif()

if(POCKET_MQTT_SANITIZE AND NOT MSVC)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=all)
    add_link_options(-fsanitize=address,undefined)
endif()

# Core library: everything except the command-line entry point.
add_library(pocket_mqtt_core STATIC
    src/BrokerEngine.cpp
    src/Logger.cpp
    src/MqttProtocol.cpp
    src/TcpServer.cpp
    src/TopicTree.cpp
)
target_include_directories(pocket_mqtt_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(pocket_mqtt_core PRIVATE pocket_mqtt_warnings)
if(WIN32)
    target_link_libraries(pocket_mqtt_core PUBLIC ws2_32)
    target_compile_definitions(pocket_mqtt_core PUBLIC _WIN32_WINNT=0x0601)
endif()
if(MSVC)
    target_compile_definitions(pocket_mqtt_core PUBLIC _CRT_SECURE_NO_WARNINGS)
endif()

add_executable(pocket-mqtt src/main.cpp)
target_link_libraries(pocket-mqtt PRIVATE pocket_mqtt_core pocket_mqtt_warnings)
target_compile_definitions(pocket-mqtt PRIVATE POCKET_MQTT_VERSION="${PROJECT_VERSION}")
install(TARGETS pocket-mqtt RUNTIME DESTINATION bin)

if(POCKET_MQTT_BUILD_TOOLS)
    find_package(Threads REQUIRED)
    add_executable(mqtt-bench tools/mqtt_bench.cpp)
    target_link_libraries(mqtt-bench PRIVATE pocket_mqtt_core pocket_mqtt_warnings Threads::Threads)
endif()

include(CTest)
if(BUILD_TESTING)
    add_executable(pocket_mqtt_tests
        tests/TestMain.cpp
        tests/ProtocolTests.cpp
        tests/TopicTreeTests.cpp
        tests/BrokerEngineTests.cpp
    )
    target_link_libraries(pocket_mqtt_tests PRIVATE pocket_mqtt_core pocket_mqtt_warnings)
    add_test(NAME pocket_mqtt_tests COMMAND pocket_mqtt_tests)
endif()
