#!/bin/bash
# Copyright 2022 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../.." || exit
BUILD_PROFILE=${BUILD_PROFILE:-debug}

echo "build_profile: $BUILD_PROFILE"

cargo build

# Caveat: has to kill query first.
# `query` tries to remove its liveness record from meta before shutting down.
# If meta is stopped, `query` will receive an error that hangs graceful
# shutdown.
killall databend-query || true
sleep 3

killall databend-meta || true
sleep 3

for bin in databend-query databend-meta; do
	if test -n "$(pgrep $bin)"; then
		echo "The $bin is not killed. force killing."
		killall -9 $bin || true
	fi
done

rm -rf ./.databend/logs-query-1
rm -rf ./.databend/logs-query-2
rm -rf ./.databend/logs-query-3

echo 'Start Meta service HA cluster(3 nodes)...'

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-1.toml &
python3 scripts/ci/wait_tcp.py --timeout 10 --port 9191

# wait for cluster formation to complete.
sleep 1

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-2.toml &
python3 scripts/ci/wait_tcp.py --timeout 10 --port 28202

# wait for cluster formation to complete.
sleep 1

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-3.toml &
python3 scripts/ci/wait_tcp.py --timeout 10 --port 28302

# wait for cluster formation to complete.
sleep 1

echo 'Start databend-query node-1'
env RUST_BACKTRACE=1 RUST_LOG=DEBUG,opendal::layers=INFO OTEL_BSP_SCHEDULE_DELAY=1 DATABEND_JAEGER_AGENT_ENDPOINT=172.17.0.3:6831 NODE_NAME=node1 nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-1.toml &

echo "Waiting on node-1..."
python3 scripts/ci/wait_tcp.py --timeout 10 --port 9091

echo 'Start databend-query node-2'
env RUST_BACKTRACE=1 RUST_LOG=DEBUG,opendal::layers=INFO OTEL_BSP_SCHEDULE_DELAY=1 DATABEND_JAEGER_AGENT_ENDPOINT=172.17.0.3:6831 NODE_NAME=node2 nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-2.toml &

echo "Waiting on node-2..."
python3 scripts/ci/wait_tcp.py --timeout 10 --port 9092

echo 'Start databend-query node-3'
env RUST_BACKTRACE=1 RUST_LOG=DEBUG,opendal::layers=INFO OTEL_BSP_SCHEDULE_DELAY=1 DATABEND_JAEGER_AGENT_ENDPOINT=172.17.0.3:6831 NODE_NAME=node3 nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-3.toml &

echo "Waiting on node-3..."
python3 scripts/ci/wait_tcp.py --timeout 10 --port 9093

echo "All done..."
