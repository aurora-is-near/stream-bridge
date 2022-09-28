#!/bin/bash

./streamcmp \
    -server "nats://producer003.nats.backend.aurora.dev:4222" \
    -creds "nats.creds" \
    -consumer "streamcmp_test" \
    -stream-a "patched_aurora_blocks" \
    -seq-a 1 \
    -stream-b "debug3_aurora_blocks" \
    -seq-b 1 \
    -mode "aurora" \
    -batch 500 \
    -rps 1
