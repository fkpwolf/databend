#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## Create table t12_0005
echo "create table t12_0005(c int)" | $MYSQL_CLIENT_CONNECT
echo "two insertions"
echo "insert into t12_0005 values(1),(2)" | $MYSQL_CLIENT_CONNECT

echo "insert into t12_0005 values(3)" | $MYSQL_CLIENT_CONNECT
echo "latest snapshot should contain 3 rows"
echo "select count(*)  from t12_0005" | $MYSQL_CLIENT_CONNECT

# alter table add a column
echo "alter table add a column"
echo "alter table t12_0005 add column a float default 1.01" | $MYSQL_CLIENT_CONNECT

## Get the previous snapshot id of the latest snapshot
SNAPSHOT_ID=$(echo "select previous_snapshot_id from fuse_snapshot('default','t12_0005') where row_count=3 " | $MYSQL_CLIENT_CONNECT)

echo "counting the data set of first insertion, which should contain 2 rows"
echo "select count(*) from t12_0005 at (snapshot => '$SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "select the data set of first insertion, which should contain 2 rows"
echo "select * from t12_0005 at (snapshot => '$SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "planner_v2: counting the data set of first insertion, which should contain 2 rows"
echo "select count(t.c) from t12_0005 at (snapshot => '$SNAPSHOT_ID') as t" | $MYSQL_CLIENT_CONNECT


# Get a time point at/after the first insertion.
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't12_0005') where row_count=2" | $MYSQL_CLIENT_CONNECT)

echo "planner_v2: counting the data set of first insertion by timestamp, which should contains 2 rows"
echo "select count(t.c) from t12_0005 at (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP) as t" | $MYSQL_CLIENT_CONNECT

echo "planner_v2: select the data set of first insertion by timestamp, which should contains 2 rows"
echo "select * from t12_0005 at (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP) as t" | $MYSQL_CLIENT_CONNECT

# alter table drop a column
echo "alter table drop a column"
echo "alter table t12_0005 drop column c" | $MYSQL_CLIENT_CONNECT

## Get the previous snapshot id of the latest snapshot
SNAPSHOT_ID=$(echo "select previous_snapshot_id from fuse_snapshot('default','t12_0005') where row_count=3 " | $MYSQL_CLIENT_CONNECT)

echo "counting the data set of first insertion, which should contain 2 rows"
echo "select count(*) from t12_0005 at (snapshot => '$SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "select the data set of first insertion, which should contain 2 rows"
echo "select * from t12_0005 at (snapshot => '$SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "planner_v2: counting the data set of first insertion, which should contain 2 rows"
echo "select count(t.c) from t12_0005 at (snapshot => '$SNAPSHOT_ID') as t" | $MYSQL_CLIENT_CONNECT


# Get a time point at/after the first insertion.
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't12_0005') where row_count=2" | $MYSQL_CLIENT_CONNECT)

echo "planner_v2: counting the data set of first insertion by timestamp, which should contains 2 rows"
echo "select count(t.c) from t12_0005 at (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP) as t" | $MYSQL_CLIENT_CONNECT

echo "planner_v2: select the data set of first insertion by timestamp, which should contains 2 rows"
echo "select * from t12_0005 at (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP) as t" | $MYSQL_CLIENT_CONNECT

## Drop table.
echo "drop table t12_0005" | $MYSQL_CLIENT_CONNECT
