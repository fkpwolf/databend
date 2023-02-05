#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists t1;" | $MYSQL_CLIENT_CONNECT

curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/query/'  --data-raw '{"sql": "select * from t1", "pagination": { "wait_time_secs": 2}}' -u root: | jq -c ".error"

curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/query/'  --data-raw '{sql": "select * from tx", "pagination": { "wait_time_secs": 2}}' -u root:
echo ""
curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/querq/'  --data-raw '{"sql": "select * from tx", "pagination": { "wait_time_secs": 2}}' -u root:
echo ""
