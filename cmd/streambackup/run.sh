#!/bin/bash

./streambackup \
    -server "nats://producer003.nats.backend.aurora.dev:4222" \
    -creds "nats.creds" \
    -consumer "streambackup_test_strokov" \
    -stream "patched_aurora_blocks" \
    -seq-start 321 \
    -seq-end 2117 \
    -mode "aurora" \
    -batch 250 \
    -rps 1 \
    -dir "backup" \
    -compress 9 \
