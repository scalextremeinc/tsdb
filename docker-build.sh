#!/bin/bash
if [ ! -z "$1" ]; then
    tag=":$1"
fi
set -x
docker build -t docker-registry.scalextreme.com/tsdb$tag -f docker/Dockerfile .
