# pocket-mqtt-broker

A zero-dependency MQTT broker written in modern C++17.

> **Status:** early development. The non-blocking TCP server (poll/WSAPoll event loop,
> RAII socket ownership, graceful close, slow-consumer protection) and a complete
> MQTT 3.1.1 packet codec are in place. The broker engine is coming next.

## Build

```sh
cmake -S . -B build
cmake --build build
./build/pocket-mqtt --port 1883 --verbose
```

## Test

```sh
ctest --test-dir build --output-on-failure
```

## License

MIT
