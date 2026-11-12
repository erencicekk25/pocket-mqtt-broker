# pocket-mqtt-broker

A zero-dependency MQTT broker written in modern C++17.

> **Status:** early development. This milestone ships the non-blocking TCP server
> (poll/WSAPoll event loop, RAII socket ownership, graceful close, slow-consumer protection).
> MQTT packet handling is coming next.

## Build

```sh
cmake -S . -B build
cmake --build build
./build/pocket-mqtt --port 1883 --verbose
```

## License

MIT
