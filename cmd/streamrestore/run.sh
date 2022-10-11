#!/bin/bash

./streamrestore \
    -server "nats://producer003.nats.backend.aurora.dev:4222" \
    -creds "nats.creds" \
    -stream "local_test" \
    -seq-start 1 \
    -seq-end 111 \
    -mode "aurora" \
    -dir "backup" \
    -backup "patched_near_blocks"
