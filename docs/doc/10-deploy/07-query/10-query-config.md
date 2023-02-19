---
title: Query Server Configurations
sidebar_label: Query Server Configurations
description:
  Query Server Configuration
---

The configuration file `databend-query.toml` contains settings for configuring the Databend query server. Additionally, you can configure more settings for the query server with the script `databend-query`. To find out the available settings with the script, refer to the script's help information:

```bash
./databend-query -h
```

This topic explains the settings you can find in the configuration file `databend-query.toml`.

:::tip
You can find [sample configuration files](https://github.com/datafuselabs/databend/tree/main/scripts/ci/deploy/config) on GitHub that set up Databend for various deployment environments. These files were created for internal testing ONLY. Please do NOT modify them for your own purposes. But if you have a similar deployment, it is a good idea to reference them when editing your own configuration files.
:::

## 1. Logging Config

### log.file

  * on: Enables or disables `file` logging. Defaults to `true`.
  * dir: Path to store log files.
  * level: Log level (DEBUG | INFO | ERROR). Defaults to `INFO`.
  * format: Log format. Defaults to `json`.
    - `json`: Databend outputs logs in JSON format.
    - `text`: Databend outputs plain text logs.

### log.stderr

  * on: Enables or disables `stderr` logging. Defaults to `false`.
  * level: Log level (DEBUG | INFO | ERROR). Defaults to `DEBUG`.
  * format: Log format. Defaults to `text`.
    - `text`: Databend outputs plain text logs.
    - `json`: Databend outputs logs in JSON format.

## 2. Meta Service Config

### username

* The username used to connect to the Meta service.
* Default: `"root"`
* Env variable: `META_USERNAME`

### password

* The password used to connect to the Meta service. Databend recommends using the environment variable to provide the password.
* Default: `"root"`
* Env variable: `META_PASSWORD`

### endpoints

* Sets one or more meta server endpoints that this query server can connect to. For a robust connection to Meta, include multiple meta servers within the cluster as backups if possible, for example, `["192.168.0.1:9191", "192.168.0.2:9191"]`.
* It is a list of `grpc_api_advertise_host:<grpc-api-port>` of databend-meta config. See [Databend-meta config: `grpc_api_advertise_host`](../06-metasrv/15-metasrv-config.md). 
* This setting only takes effect when Databend works in cluster mode. You don't need to configure it for standalone Databend.
* Default: `["0.0.0.0:9191"]`
* Env variable: `META_ENDPOINTS`

### client_timeout_in_second

* Sets the wait time (in seconds) before terminating the attempt to connect a meta server.
* Default: 60

### auto_sync_interval

* Sets how often (in seconds) this query server should automatically sync up `endpoints` from the meta servers within the cluster. When enabled, databend-query tries to contact one of the databend-meta server to get a list of `grpc_api_advertise_host:<grpc-api-port>` periodically.
* If a databend-meta is **NOT** configured with `grpc_api_advertise_host`, it fills blank string `""` in the returned endpoint list. See [Databend-meta config: `grpc_api_advertise_host`](../06-metasrv/15-metasrv-config.md).
* If the returned endpoints list contains more than half invalid addresses, e.g., 2/3 are `""`: `["127.0.0.1:9191", "",""]`, databend-query will not update the `endpoint`.
* To disable the sync up, set it to 0.
* This setting only takes effect when Databend-query works with remote meta service(`endpoints` is not empty). You don't need to configure it for standalone Databend.
* Default: 60

## 3. Query config

### admin_api_address

* The IP address and port to listen on for admin the databend-query, e.g., `0.0.0.0::8080`.
* Default: `"127.0.0.1:8080"`
* Env variable: `QUERY_ADMIN_API_ADDRESS`

### metric_api_address

* The IP address and port to listen on that can be scraped by Prometheus, e.g., `0.0.0.0::7070`.
* Default: `"127.0.0.1:7070"`
* Env variable: `QUERY_METRIC_API_ADDRESS`

### flight_api_address

* The IP address and port to listen on for databend-query cluster shuffle data, e.g., `0.0.0.0::9090`.
* Default: `"127.0.0.1:9090"`
* Env variable: `QUERY_FLIGHT_API_ADDRESS`

### mysql_handler_host

* The IP address to listen on for MySQL handler, e.g., `0.0.0.0`.
* Default: `"127.0.0.1"`
* Env variable: `QUERY_MYSQL_HANDLER_HOST`

### mysql_handler_port

* The port to listen on for MySQL handler, e.g., `3307`.
* Default: `3307`
* Env variable: `QUERY_MYSQL_HANDLER_PORT`

### clickhouse_handler_host

* The IP address to listen on for ClickHouse handler, e.g., `0.0.0.0`.
* Default: `"127.0.0.1"`
* Env variable: `QUERY_CLICKHOUSE_HANDLER_HOST`

### clickhouse_http_handler_host

* The IP address to listen on for ClickHouse HTTP handler, e.g., `0.0.0.0`.
* Default: `"127.0.0.1"`
* Env variable: `QUERY_CLICKHOUSE_HTTP_HANDLER_HOST`

### clickhouse_http_handler_port

* The port to listen on for ClickHouse HTTP handler, e.g., `8124`.
* Default: `8124`
* Env variable: `QUERY_CLICKHOUSE_HTTP_HANDLER_PORT`

### tenant_id

* Identifies the tenant and is used for storing the tenant's metadata.
* Default: `"admin"`
* Env variable: `QUERY_TENANT_ID`

### cluster_id

* Identifies the cluster that the databend-query node belongs to.
* Default: `""`
* Env variable: `QUERY_CLUSTER_ID`


## 4. Storage config

### type

* Which storage type(Must one of `"fs"` | `"s3"` | `"azblob"` | `"obs"`) should use for the databend-query, e.g., `"s3"`.
* Default: `""`
* Env variable: `STORAGE_TYPE`
* Required.

### storage.s3

#### bucket

* AWS S3 bucket name.
* Default: `""`
* Env variable: `STORAGE_S3_BUCKET`
* Required.

#### endpoint_url

* AWS S3(or MinIO S3-like) endpoint URL, e.g., `"https://s3.amazonaws.com"`.
* Default: `"https://s3.amazonaws.com"`
* Env variable: `STORAGE_S3_ENDPOINT_URL`

#### access_key_id

* AWS S3 access_key_id.
* Default: `""`
* Env variable: `STORAGE_S3_ACCESS_KEY_ID`
* Required.

#### secret_access_key

* AWS S3 secret_access_key.
* Default: `""`
* Env variable: `STORAGE_S3SECRET_ACCESS_KEY`
* Required.

### storage.azblob

#### endpoint_url

* Azure Blob Storage endpoint URL, e.g., `"https://<your-storage-account-name>.blob.core.windows.net"`.
* Default: `""`
* Env variable: `STORAGE_AZBLOB_ENDPOINT_URL`
* Required.

#### container

* Azure Blob Storage container name.
* Default: `""`
* Env variable: `STORAGE_AZBLOB_CONTAINER`
* Required.

#### account_name

* Azure Blob Storage account name.
* Default: `""`
* Env variable: `STORAGE_AZBLOB_ACCOUNT_NAME`
* Required.

#### account_key

* Azure Blob Storage account key.
* Default: `""`
* Env variable: `STORAGE_AZBLOB_ACCOUNT_KEY`
* Required.

### storage.obs

#### bucket

* OBS bucket name.
* Default: `""`
* Env variable: `STORAGE_OBS_BUCKET`
* Required.

#### endpoint_url

* OBS endpoint URL, e.g., `"https://obs.cn-north-4.myhuaweicloud.com"`.
* Default: `""`
* Env variable: `STORAGE_OBS_ENDPOINT_URL`
* Required.

#### access_key_id

* OBS access_key_id.
* Default: `""`
* Env variable: `STORAGE_OBS_ACCESS_KEY_ID`
* Required.

#### secret_access_key

* OBS secret_access_key.
* Default: `""`
* Env variable: `STORAGE_OBS_SECRET_ACCESS_KEY`
* Required.

## 5. Cache

:::note

This need databend-query version >= v0.9.40-nightly.

:::

### data_cache_storage

* Type of storage to keep the table data cache, set to `disk` to enable the disk cache.
* Default: `"none"`
* Env variable: `DATA_CACHE_STORAGE`
 
### cache.disk

#### path

* Table disk cache root path.
* Default: `"./.databend/_cache"`
* Env variable: `CACHE-DISK-PATH`

#### max_bytes

* Max bytes of cached raw table data.
* Default: `21474836480`
* Env variable: `CACHE-DISK-MAX-BYTES`

### Cache Config Example

Enable disk cache:
```shell

...

data_cache_storage = "disk"
[cache.disk]
# cache path
path = "./databend/_cache"
# max bytes of cached data 20G
max_bytes = 21474836480
```

## A Full databend-query.toml Config File Sample

For ease of experience, set all hosts to 0.0.0.0. Exercise caution when setting host if the application is in production.

```toml title="databend-query.toml"
# Logging
[log.file]
on = true
dir = "./.datanend/logs"
level = "INFO"
format = "json"

[log.stderr]
on = false
level = "DEBUG"
format = "text"

# Meta Service
[meta]
endpoints = ["0.0.0.0:9191"]
username = "root"
password = "root"
client_timeout_in_second = 60
auto_sync_interval = 60

[query]
# For admin RESET API.
admin_api_address = "0.0.0.0:8001"

# Metrics.
metric_api_address = "0.0.0.0:7071"

# Cluster flight RPC.
flight_api_address = "0.0.0.0:9091"

# Query MySQL Handler.
mysql_handler_host = "0.0.0.0"
mysql_handler_port = 3307

# Query ClickHouse Handler.
clickhouse_handler_host = "0.0.0.0"
clickhouse_handler_port = 9001

# Query HTTP Handler.
http_handler_host = "0.0.0.0"
http_handler_port = 8000

tenant_id = "tenant1"
cluster_id = "cluster1"

[storage]
# s3
type = "s3"

[storage.s3]
bucket = "databend"
endpoint_url = "https://s3.amazonaws.com"
access_key_id = "<your-key-id>"
secret_access_key = "<your-access-key>"
```