# Blockwriter

## BlockWriter

Having that "block-stream" is:
- JetStream with possibly enabled deduplication by `Nats-Msg-Id` header.
- Every message is a block.
- `Nats-Msg-Id` header is equal to block height.
- Every block has `height`, `hash` and `prevHash`, where `prevHash` is always equal to the `hash` of previous message.

`BlockWriter` is a utility that allows to safely write to such stream without violating those conditions.

Constructor of `BlockWriter` receives config, target stream and a function that is able to parse block from NATS message (needed for fetching a tip of the given stream).

First of all, it allows to fetch the current stream tip (useful for figuring out where to start) as an `AbstractBlock`.
Note that `GetTip` function caches the result, so it also expects `ttl` argument, meaning that returned result will not be older than `ttl`.

And mainly, it allows to write to the stream. `Write` function receives block description as an `AbstractBlock` (for performing `height` and `hash` checks) and final block payload itself (`data`).
Here's what `Write` does:
- It compares provided block against last known tip. *Note*: knowledge about tip is updated from both previous writes and automatical fetch each `TipTtlSeconds`. In general it's not a problem to have outdated tip knowledge as long as current writing height falls into deduplication window. So it's recommended to have `TipTtlSeconds` to be not greater than something like a half of dedup window.
- If it's not higher by `height` than tip - `ErrLowHeight` is returned (usually it means that some other instance already wrote next block, nothing critical).
- If it's higher by `height` but `prevHash` of the block is not equal to `tip.hash` - `ErrHashMismatch` is returned.
- Then it attempts to write given block into stream using `MaxWriteAttempts` with `WriteRetryWaitMs` cooldown.
- During write, two safety headers are also added to the message in order to prevent shuffling/deduplication/etc - `ExpectedLastMsgIdHdr` and `ExpectedLastSeqHdr`. *Note*: there are rare use-cases of manual intervention into the stream when those checks are wanted to be disabled, that can be done by setting `DisableExpectedCheck = nextSeq` or `DisableExpectedCheckHeight = nextHeight`.

## VanillaBlockWriter

Used for testing purposes.
