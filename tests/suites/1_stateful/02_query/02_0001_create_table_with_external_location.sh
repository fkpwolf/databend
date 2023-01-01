#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists table_external_location;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists table_external_location_with_location_prefix;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table table_external_location(a int) 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT
echo "create table table_external_location_with_location_prefix(a int) 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') location_prefix = 'lulu_';" | $MYSQL_CLIENT_CONNECT

table_inserts=(
  "insert into table_external_location(a) values(888)"
  "insert into table_external_location(a) values(1024)"
  "insert into table_external_location_with_location_prefix(a) values(888)"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
done

## Select table
echo "select * from table_external_location order by a;" | $MYSQL_CLIENT_CONNECT
## select block_location and get part_prefix, block_location like this: 1/1209/_b/lulu_ca5ebf54bf894f4bb1ee232c1a0461a2_v0.parquet
echo "select block_location from fuse_block('default','table_external_location_with_location_prefix');" | $MYSQL_CLIENT_CONNECT |  cut -d "/" -f  4 | cut -d "_" -f 1

## Drop table
echo "drop table if exists table_external_location;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists table_external_location_with_location_prefix;" | $MYSQL_CLIENT_CONNECT

