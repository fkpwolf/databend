---
title: ClickHouse Handler
sidebar_label: ClickHouse Handler
description:
  Databend is ClickHouse HTTP API wire protocol-compatible.
---

![image](/img/api/api-handler-clickhouse.png)

## Overview

Databend is ClickHouse HTTP API wire protocol-compatible, allowing users and developers to easily connect to Databend with ClickHouse HTTP Handler.

## ClickHouse REST API

:::tip
Databend ClickHouse HTTP handler is a simplified version of the implementation, it only providers:
* Heath check
* Insert with JSONEachRow format
:::

### Health Check

```sql title='query=SELECT 1'
curl '127.0.0.1:8124/?query=select%201'
```

```sql title='Response'
1
```

### Insert with JSONEachRow(ndjson)

:::note
** Databend ClickHouse HTTP handler only supports put ndjson(JSONEachRow in ClickHouse) format values**.
:::

ndjson is a newline delimited JSON format:
* Line Separator is '\n' 
* Each Line is a Valid JSON Value

For example, we have a table:
```sql title='table t1'
CREATE TABLE t1(a TINYINT UNSIGNED);
```

Insert into `t1`:
```shell title='insert into t1 format JSONEachRow'
echo -e '{"a": 1}\n{"a": 2}' | curl '127.0.0.1:8124/?query=INSERT%20INTO%20t1%20FORMAT%20JSONEachRow' --data-binary @-
```

### Insert with Authentication

Use HTTP basic authentication:
```shell
echo -e '{"a": 1}\n{"a": 2}' | curl 'user:password@127.0.0.1:8124/?query=INSERT%20INTO%20t1%20FORMAT%20JSONEachRow' --data-binary @-
```

### Compression

Databend ClickHouse HTTP handler supports the following compression methods:
* BR
* DEFLATE
* GZIP
