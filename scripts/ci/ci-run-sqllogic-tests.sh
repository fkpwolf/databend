#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Starting standalone DatabendQuery and DatabendMeta"
./scripts/ci/deploy/databend-query-standalone.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests/logictest" || exit

TEST_HANDLERS=${TEST_HANDLERS:-"mysql,http,clickhouse"}

RUN_DIR=""
if [ $# -gt 0 ]; then
	RUN_DIR="--run-dir $*"
fi
echo "Run suites using argument: $RUN_DIR"
echo -e "ulimit:\n$(ulimit -a)"

echo "pip list"
python3 -m pip list

echo "Starting databend-sqllogic tests"
python3 main.py --handlers ${TEST_HANDLERS} --skip-dir=mode ${RUN_DIR}

echo "Starting databend-sqllogic mode standalone"
python3 main.py --handlers ${TEST_HANDLERS} --suite suites/mode --run-dir standalone
