#!/bin/bash

./streamcmp \
    -server "nats://producer003.nats.backend.aurora.dev:4222" \
    -creds "nats.creds" \
    -consumer "streamcmp_test" \
    -stream-a "patched_aurora_blocks" \
    -seq-a 11847000 \
    -stream-b "debug3partial_aurora_blocks" \
    -seq-b 11847000 \
    -mode "aurora" \
    -batch 500 \
    -rps 1 \
    -skip-duplicates \
    -skip-unequal-duplicates \
    -skip-gaps \
    -skip-downjumps \
    -skip-corrupted \
    -skip-discrepancy
