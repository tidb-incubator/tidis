# tikv-service

`tikv-service` is the service layer for TiKV, a distributed key-value storage powered by PingCAP.

## Running

The repository provides a server, client library, and some client executables
for interacting with the server.

Start the server:

```
RUST_LOG=debug cargo run --bin tikv-service
```

