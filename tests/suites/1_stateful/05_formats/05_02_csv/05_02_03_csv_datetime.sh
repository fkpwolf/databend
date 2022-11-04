#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

cat << EOF > /tmp/databend_test_csv1.txt
"2023-04-08 01:01:01","Hello",123
"2023-02-03 02:03:02","World",123

EOF

cat << EOF > /tmp/databend_test_csv2.txt
"2023-04-08 01:01:01","Hello",123
"2023-023-02 02:03:02","World",123

EOF


echo "drop table if exists a;" | $MYSQL_CLIENT_CONNECT

echo "create table a ( a datetime, b string, c int);" | $MYSQL_CLIENT_CONNECT

curl -sH "insert_sql:insert into a format Csv" -H "format_skip_header:0" -u root:  -F "upload=@/tmp/databend_test_csv1.txt"  -F "upload=@/tmp/databend_test_csv2.txt" -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "Date type"

echo "drop table a;" | $MYSQL_CLIENT_CONNECT
