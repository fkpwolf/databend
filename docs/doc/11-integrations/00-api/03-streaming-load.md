---
title: Streaming Load API
sidebar_label: Streaming Load API
description:
  Streaming Load API
---

The Streaming Load API is used to read data from your local files and load it into Databend.

![image](/img/load/load-data-from-local-fs.png)

## Creating an API Request

To create a request with the Streaming Load API, follow the format below:

```bash
curl -H "insert_sql:<value>" -F "upload=@<file_location>" [-F "upload=@<file_location>"] -XPUT http://<user_name>:[password]@<http_handler_host>:<http_handler_port>/v1/streaming_load
```

The parameter `insert_sql` is required and must include an INSERT statement as well as the `FILE_FORMAT` parameter that specifies the file formats. For details about `FILE_FORMAT`, see [Input & Output File Formats](../../13-sql-reference/50-file-format-options.md).

| Parameter               | Values                              | Supported Formats         | Examples                                                                                                                              |
|-------------------------|-------------------------------------|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| insert_sql              | [INSERT_statement] + [FILE_FORMAT] | All                       | -H "insert_sql: insert into ontime file_format = (type = CSV skip_header = 1 compression = 'bz2')"                                                                                        |                                                                                                                                                                                          | CSV                       |                                                                                                                                       |


## Alternatives to Streaming Load API

The [COPY INTO](../../14-sql-commands/10-dml/dml-copy-into-table.md) command enables you to load data from files using insecure protocols, such as HTTP. This simplifies the data loading in some specific scenarios, for example, Databend is installed on-premises with MinIO. In such cases, you can load data from local files with the COPY INTO command. 

Example:

```sql
COPY INTO ontime200 FROM 'fs://<file_path>/ontime_200.csv' FILE_FORMAT = (type = CSV field_delimiter = ','  record_delimiter = '\n' skip_header = 1);
```
To do so, you must add the setting `allow_insecure` to the configuration file `databend-query.toml` as indicated below and set it to `true`:

```toml
...
[storage]
...
allow_insecure = true
...
```

:::caution
For security reasons, Databend does NOT recommend insecure protocols for data loading. Use them for tests only. DO NOT set `allow_insecure` to `true` in any production environment. 
:::
