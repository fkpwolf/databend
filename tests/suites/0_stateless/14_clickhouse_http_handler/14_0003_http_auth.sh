#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d 'drop user if exists user1'

curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "create user user1 identified by 'abc123'"

curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "grant select on *.* to 'user1'"

curl -s -u user1:abc123 -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d 'select 1 FORMAT CSV'

curl -s -H 'X-ClickHouse-User: user1' -H 'X-ClickHouse-Key: abc123' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d 'select 1 FORMAT CSV'

curl -s -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}?user=user1&password=abc123" -d 'select 1 FORMAT CSV'
