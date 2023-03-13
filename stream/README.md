# Stream

## Stream

A basic wrapper for working with NATS JetStream.
- Encapsulates NATS connection, connects to the NATS endpoint, gets JS context, validates that stream exists. Note: if subject (for publishing) is not provided, it will be automatically figured out by fetching the stream info. In case if stream is a mirror of another stream (and hence has no own subject) - subject will be inherited from the origin (or origin of the origin and so on). If stream has multiple subjects, the first one is selected.
- Allows to get stream info. Note: `GetInfo` function caches the result and hence has `ttl` argument.
- Allows to get stream message by sequence.
- Allows to publish a message to the stream. Note: `nats.ExpectedStreamHdr` is added to the message for precise publishing.
- Allows to get nats connection stats (used for I/O read/write metrics).

## Reader

Reads from the stream (using pull consumer), encapsulates a lot of error/wrong data handling. In general provides more reading consistency guarantees to the caller.
- On start: if it exists, deletes the durable consumer (which is the only consumer type supported by a pull subscription) and subscribes to the stream starting from the provided sequence (`startSeq`).
- Reading: requests the next batch with a frequency defined by `MaxRps` (max requests per second).
- Batch size is calculated based on knowledge about number of available messages. This knowledge is obtained from finding the difference between the current reading sequence and the last sequence of the stream (available from `stream.GetInfo()`), which is fetched not more often than `LastSeqUpdateIntervalSeconds`. Maximum batch size is restricted by `MaxRequestBatchSize`.
- If `SortBatch` option is provided, messages of every obtained batch are sorted within that batch before consumption, in order to prevent eventual shuffles.
- Reader guarantees that messages are following consecutively without gaps or reorderings. Which means that sequence of `i`-th message is always a sequence of `i-1`-th message `+ 1`. In case if reader obtains a message which has unexpected sequence (for example 1, 2, 3 and then 17) - this message is ignored and not given to the caller. `WrongSeqToleranceWindow` option defines how many consecutive obtained messages are allowed to ignore, and in case if this window is exceeded - error is returned to the caller.
- If `StrictStart` is provided, reader will return error to the caller in case if first obtained message sequence is not equal to the provided `startSeq`. Disabling this check is **only** useful for streams that have their tail dynamically removed (which leads to impossibility to guarantee of reading start from some exact sequence) - this case is perhaps very rare, so `StrictStart` must nearly always be equal to `true`.
- `Reader.Output()` returns channel that returns `*ReaderOutput` objects. Each `ReaderOutput` either has message and it's metadata, or error which means that execution of reader has failed and stopped.
- `Reader.Stop()` will immediately stop reader's execution and unsubscribe.

## AutoReader

A very simple wrapper around `Reader`, which never returns error to the caller, but instead does reconnection if any error happened. Reconnection delay is defined by `ReconnectWaitMs`. Has the same output interface as `Reader`.
