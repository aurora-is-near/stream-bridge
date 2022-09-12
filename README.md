# stream-bridge
Go-tool for copying Aurora/Near block JetStreams, using content verification and independent NATS configurations.

## Modes
- `aurora` - will retrieve height, hash and prevHash from aurora-type blocks.
- `near` - will retrieve height, hash and prevHash from near-type blocks.
- `unverified` - will retrieve height from `Nats-Msg-Id` header, hash-checks will not be performed.

## Non-obvious config fields
- `[Input|Output].Subject` - can be left empty, than will be figured automatically.
- `Reader.MaxRps` - maximum pull consumer fetches per second.
- `Reader.BufferSize` - how many blocks will be prefetched in advance (useful for async decoupling of reader/writer goroutines).
- `Reader.InactiveThresholdSeconds` - used for consumer to survive long reconnection.
- `Reader.SortBatch` - sorts messages by sequence id within each batch (I wasn't sure if it's sorted by default).
- `Reader.LastSeqUpdateIntervalSeconds` - defines frequency of input stream size fetch, the more frequent it is - the more precise batch size is selected (when close to the tip).
- `Reader.StrictStart` - if true, will ensure that first message seq is equal to startSeq. Only useful for unverified mode.
- `Writer.TipTtlSeconds` - defines how frequently writer will refresh information about the actual output tip. Needed to not fall out of dedup window. If lower than dedupWindow / 2 - will be automatically lowered.
- `InputStartSequence` - inclusive, set `0` to start from beginning.
- `InputEndSequence` - exclusive, set `0` to never end.
- `RestartDelayMs` - if connection was lost, this amount of milliseconds will be waited before starting next reconnection.
- `ToleranceWindow` - Maximum number of consecutive blocks that are corrupted, or blocks whose `prev_hash` doesn't match the hash of last block in output stream. After tolerance window is exceeded, program exits with error.
- `Metrics.Labels` - absolutely custom keys and values, sysadmin is free to arrange it in a way that will help him distinguishing different metrics in grafana.

## Pipeline lifecycle
1. Pipeline starts.
2. On any read/write error pipeline will be stopped, reconnection attempt will be done after `RestartDelayMs`, and then pipeline will start again.
3. If last block in output stream is present but can't be decoded - program exits with an error.
4. If pipeline gets too many (see `ToleranceWindow` option) consecutive blocks that don't make sense - program exits with an error.
5. If pipeline reaches the `InputEndSequence` and successfully writes it - program stops gracefully.
6. On `SIGHUP`, `SIGTERM`, `SIGQUIT`, `SIGABRT`, `SIGINT`, `SIGUSR1` - program stops gracefully.
7. If metrics server dies - program exits with an error.
