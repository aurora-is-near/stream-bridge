#!/bin/bash

# https://github.com/aurora-is-near/devops-stuff/tree/main/docker/protoc
docker run --rm -v "$(pwd)"/:/proto protoc --gogoslick_out=/proto/. messagebackup.proto --proto_path=/proto/
