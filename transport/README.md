# Transport

## NatsConnection

That’s a simple wrapper around NATS connection.
- `ConnectNats` function receives config (and fills some defaults for empty fields) and, optionally, `errorChan` for communicating errors (in practice it’s almost never needed).
- Allows to drain the connection.
- Allows to get the underlying connection.
- Allows to asynchronously understand when connection is closed (never happens if `MaxReconnects == -1`).
- Logs every error/warning that happens.
