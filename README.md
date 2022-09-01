# stream-bridge
Go-tool for copying Aurora/Near block JetStreams, using content verification and independent NATS configurations.

## Non-obvious config fields
- `Reader.MaxRps` - maximum pull consumer fetches per second.
- `Reader.BufferSize` - how many blocks will be prefetched in advance (useful for async decoupling of reader/writer goroutines).
- `Reader.InactiveThresholdSeconds` - used for consumer to survive long reconnection.
- `Reader.SortBatch` - sorts messages by sequence id within each batch (I wasn't sure if it's sorted by default).
- `Reader.LastSeqUpdateIntervalSeconds` - defines frequency of input stream size fetch, the more frequent it is - the more precise batch size is selected.
- `InputStartSequence` - inclusive, set `0` to start from beginning.
- `InputEndSequence` - inclusive, set `0` to never end.
- `RestartDelayMs` - if pipeline has failed, this amount of milliseconds will be waited before starting next try.
- `ForceRestartAfterSeconds` - forces pipeline to restart after given number of seconds. Can be used to prevent falling out of deduplication time window. It's recommended to set it to something like `dedup_window / 2`. `0` means no force restart.
- `ToleranceWindow` - Maximum number of consecutive blocks that are corrupted, or blocks whose `prev_hash` doesn't match the hash of last block in output stream. After tolerance window is exceeded, program exits with error.
- `Metrics.Labels` - absolutely custom keys and values, sysadmin is free to arrange it in a way that will help him distinguishing different metrics in grafana.

## Pipeline lifecycle
1. Pipeline starts
2. If last block in output stream is present but can't be decoded - program exits with an error.
3. If there's a error of connection/reading/writing (or it's running for `ForceRestartAfterSeconds` if provided) - it gracefully closes all NATS connections, stops goroutines, frees memory etc; then waits for `RestartDelayMs` and starts again.
4. If pipeline gets too many (see `ToleranceWindow` option) consecutive blocks that don't make sense - program exits with an error.
5. If pipeline reaches the `InputEndSequence` and successfully writes it - program stops gracefully.
6. On `SIGHUP`, `SIGTERM`, `SIGQUIT`, `SIGABRT`, `SIGINT`, `SIGUSR1` - program stops gracefully.
7. If metrics server dies - program exits with an error.
