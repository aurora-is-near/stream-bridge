# Streambridge

## StreamBridge

Copies from given input stream to given output stream. Is able to work with 3 block types:
- Aurora block
- NEAR block
- Unverified: `height` is derived from `Nats-Msg-Id` header; `hash` is always empty (no hash-verification)

Algorithm:
1. Connect both streams.
2. Start `BlockWriter` for the output stream.
3. Figure out the tip of the output stream from `BlockWriter`.
4. Find position in the input stream that matches the output tip (see `calculateStartSeq`).
5. Start `Reader` for the input stream.
6. Copy every block returned from `Reader` (input) to the `BlockWriter` (output).
7. If input block can not be parsed, it is ignored (and counted as wrong).
8. If `BlockWriter` returns `ErrLowHeight`, input block is ignored (without counting as wrong, just skipping).
9.  If `BlockWriter` returns `ErrHashMismatch`, input block is ignored (and counted as wrong).
10. If number of consecutive wrong blocks ever reaches `ToleranceWindow` - finish with error.
11. If any stream-connection-related error happens - that stream is disconnected and loop is restarted from step [1] after sleeping for `RestartDelayMs`.
12. If any other error happens - execution finishes with that error.
13. If `Reader` reaches the end (`InputEndSequence`) - execution finishes gracefully.

`calculateStartSeq` algorithm:
1. `lowerBound = min(max(InputStartSequence, input.FirstSeq), input.LastSeq)`.
2. `upperBound = min(max(InputEndSequence - 1, input.FirstSeq), input.LastSeq)`.
3. Start from the `upperBound`.
4. Check height of the block on the current position.
5. If it's lower or equal than needed, return this position.
6. If it's higher than needed, jump down by height difference, but not further than `lowerBound`, and goto [4].
