#!/bin/bash

set -e

export VERSION_TAG=${VERSION_TAG:-v2021.07.01}
export UID_GID="$(id -u):$(id -g)"

docker-compose down || true
docker-compose up
