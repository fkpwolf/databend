// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::env::temp_dir;
use std::fs;
use std::io::Write;

use common_config::CatalogConfig;
use common_config::CatalogHiveConfig;
use common_config::Config;
use common_config::ThriftProtocol;
use common_exception::Result;
use pretty_assertions::assert_eq;

// Default.
#[test]
fn test_default_config() -> Result<()> {
    let actual = Config::default();

    let tom_expect = r#"cmd = ""
config_file = ""

[query]
tenant_id = "admin"
cluster_id = ""
num_cpus = 0
mysql_handler_host = "127.0.0.1"
mysql_handler_port = 3307
max_active_sessions = 256
max_server_memory_usage = 0
max_memory_limit_enabled = false
clickhouse_handler_host = "127.0.0.1"
clickhouse_handler_port = 9000
clickhouse_http_handler_host = "127.0.0.1"
clickhouse_http_handler_port = 8124
http_handler_host = "127.0.0.1"
http_handler_port = 8000
http_handler_result_timeout_secs = 60
flight_api_address = "127.0.0.1:9090"
admin_api_address = "127.0.0.1:8080"
metric_api_address = "127.0.0.1:7070"
http_handler_tls_server_cert = ""
http_handler_tls_server_key = ""
http_handler_tls_server_root_ca_cert = ""
api_tls_server_cert = ""
api_tls_server_key = ""
api_tls_server_root_ca_cert = ""
rpc_tls_server_cert = ""
rpc_tls_server_key = ""
rpc_tls_query_server_root_ca_cert = ""
rpc_tls_query_service_domain_name = "localhost"
table_engine_memory_enabled = true
database_engine_github_enabled = true
wait_timeout_mills = 5000
max_query_log_size = 10000
table_cache_enabled = false
table_cache_block_meta_count = 102400
table_memory_cache_mb_size = 256
table_disk_cache_root = "_cache"
table_disk_cache_mb_size = 1024
table_cache_snapshot_count = 256
table_cache_statistic_count = 256
table_cache_segment_count = 10240
table_cache_bloom_index_meta_count = 3000
table_cache_bloom_index_data_bytes = 1073741824
management_mode = false
jwt_key_file = ""
async_insert_max_data_size = 10000
async_insert_busy_timeout = 200
async_insert_stale_timeout = 0
users = []
share_endpoint_address = ""
share_endpoint_auth_token_file = ""

[log]
level = "INFO"
dir = "./.databend/logs"
query_enabled = false

[log.file]
on = true
level = "INFO"
dir = "./.databend/logs"
format = "json"

[log.stderr]
on = false
level = "INFO"
format = "text"

[meta]
embedded_dir = ""
endpoints = []
username = "root"
password = ""
client_timeout_in_second = 10
auto_sync_interval = 10
rpc_tls_meta_server_root_ca_cert = ""
rpc_tls_meta_service_domain_name = "localhost"

[storage]
type = "fs"
num_cpus = 0
allow_insecure = false

[storage.fs]
data_path = "_data"

[storage.gcs]
endpoint_url = "https://storage.googleapis.com"
bucket = ""
root = ""
credential = ""

[storage.s3]
region = ""
endpoint_url = "https://s3.amazonaws.com"
access_key_id = ""
secret_access_key = ""
security_token = ""
bucket = ""
root = ""
master_key = ""
enable_virtual_host_style = false
role_arn = ""
external_id = ""

[storage.azblob]
account_name = ""
account_key = ""
container = ""
endpoint_url = ""
root = ""

[storage.hdfs]
name_node = ""
root = ""

[storage.obs]
access_key_id = ""
secret_access_key = ""
bucket = ""
endpoint_url = ""
root = ""

[storage.oss]
access_key_id = ""
access_key_secret = ""
bucket = ""
endpoint_url = ""
root = ""

[storage.cache]
type = "none"
num_cpus = 0

[storage.cache.fs]
data_path = "_data"

[storage.cache.moka]
max_capacity = 1073741824
time_to_live = 3600
time_to_idle = 600

[storage.cache.redis]
endpoint_url = ""
username = ""
password = ""
root = ""
db = 0
default_ttl = 0

[catalog]
address = ""
protocol = ""

[catalogs]
"#;

    let tom_actual = toml::to_string(&actual.into_outer()).unwrap();
    assert_eq!(tom_expect, tom_actual);

    Ok(())
}

// From env, defaulting.
#[test]
fn test_env_config_s3() -> Result<()> {
    temp_env::with_vars(
        vec![
            ("LOG_LEVEL", Some("DEBUG")),
            ("QUERY_TENANT_ID", Some("tenant-1")),
            ("QUERY_CLUSTER_ID", Some("cluster-1")),
            ("QUERY_MYSQL_HANDLER_HOST", Some("127.0.0.1")),
            ("QUERY_MYSQL_HANDLER_PORT", Some("3306")),
            ("QUERY_MAX_ACTIVE_SESSIONS", Some("255")),
            ("QUERY_CLICKHOUSE_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HANDLER_PORT", Some("9000")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_PORT", Some("8124")),
            ("QUERY_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_HTTP_HANDLER_PORT", Some("8001")),
            ("QUERY_FLIGHT_API_ADDRESS", Some("1.2.3.4:9091")),
            ("QUERY_ADMIN_API_ADDRESS", Some("1.2.3.4:8081")),
            ("QUERY_METRIC_API_ADDRESS", Some("1.2.3.4:7071")),
            ("QUERY_TABLE_CACHE_ENABLED", Some("true")),
            ("QUERY_TABLE_MEMORY_CACHE_MB_SIZE", Some("512")),
            ("QUERY_TABLE_DISK_CACHE_ROOT", Some("_cache_env")),
            ("QUERY_TABLE_DISK_CACHE_MB_SIZE", Some("512")),
            ("QUERY_TABLE_CACHE_SNAPSHOT_COUNT", Some("256")),
            ("QUERY_TABLE_CACHE_SEGMENT_COUNT", Some("10240")),
            ("META_ENDPOINTS", Some("0.0.0.0:9191")),
            ("TABLE_CACHE_BLOOM_INDEX_META_COUNT", Some("3000")),
            (
                "TABLE_CACHE_BLOOM_INDEX_DATA_BYTES",
                Some(format!("{}", 1024 * 1024 * 1024).as_str()),
            ),
            ("STORAGE_TYPE", Some("s3")),
            ("STORAGE_NUM_CPUS", Some("16")),
            ("STORAGE_FS_DATA_PATH", Some("/tmp/test")),
            ("STORAGE_S3_REGION", Some("us.region")),
            ("STORAGE_S3_ENDPOINT_URL", Some("http://127.0.0.1:10024")),
            ("STORAGE_S3_ACCESS_KEY_ID", Some("us.key.id")),
            ("STORAGE_S3_SECRET_ACCESS_KEY", Some("us.key")),
            ("STORAGE_S3_BUCKET", Some("us.bucket")),
            (
                "STORAGE_GCS_ENDPOINT_URL",
                Some("http://gcs.storage.cname_map.local"),
            ),
            ("STORAGE_GCS_BUCKET", Some("gcs.bucket")),
            ("STORAGE_GCS_ROOT", Some("/path/to/root")),
            ("STORAGE_GCS_CREDENTIAL", Some("gcs.credential")),
            ("STORAGE_OSS_BUCKET", Some("oss.bucket")),
            (
                "STORAGE_OSS_ENDPOINT_URL",
                Some("https://oss-cn-litang.example.com"),
            ),
            ("STORAGE_OSS_ROOT", Some("oss.root")),
            ("STORAGE_OSS_ACCESS_KEY_ID", Some("access_key_id")),
            ("STORAGE_OSS_ACCESS_KEY_SECRET", Some("access_key_secret")),
            ("QUERY_TABLE_ENGINE_MEMORY_ENABLED", Some("true")),
            ("CONFIG_FILE", None),
        ],
        || {
            let configured = Config::load_for_test().expect("must success").into_outer();

            assert_eq!("DEBUG", configured.log.level);

            assert_eq!("tenant-1", configured.query.tenant_id);
            assert_eq!("cluster-1", configured.query.cluster_id);
            assert_eq!("127.0.0.1", configured.query.mysql_handler_host);
            assert_eq!(3306, configured.query.mysql_handler_port);
            assert_eq!(255, configured.query.max_active_sessions);
            assert_eq!("1.2.3.4", configured.query.clickhouse_http_handler_host);
            assert_eq!(8124, configured.query.clickhouse_http_handler_port);
            assert_eq!("1.2.3.4", configured.query.http_handler_host);
            assert_eq!(8001, configured.query.http_handler_port);

            assert_eq!("1.2.3.4:9091", configured.query.flight_api_address);
            assert_eq!("1.2.3.4:8081", configured.query.admin_api_address);
            assert_eq!("1.2.3.4:7071", configured.query.metric_api_address);

            assert_eq!(1, configured.meta.endpoints.len());
            assert_eq!("0.0.0.0:9191", configured.meta.endpoints[0]);

            assert_eq!("s3", configured.storage.storage_type);
            assert_eq!(16, configured.storage.storage_num_cpus);

            // config of fs should not be loaded, take default value.
            assert_eq!("_data", configured.storage.fs.data_path);

            // Storage type is fs, gcs related value should be default.
            assert_eq!(
                "https://storage.googleapis.com",
                configured.storage.gcs.gcs_endpoint_url
            );
            assert_eq!("", configured.storage.gcs.gcs_bucket);
            assert_eq!("", configured.storage.gcs.gcs_root);
            assert_eq!("", configured.storage.gcs.credential);

            assert_eq!("us.region", configured.storage.s3.region);
            assert_eq!("http://127.0.0.1:10024", configured.storage.s3.endpoint_url);
            assert_eq!("us.key.id", configured.storage.s3.access_key_id);
            assert_eq!("us.key", configured.storage.s3.secret_access_key);
            assert_eq!("us.bucket", configured.storage.s3.bucket);

            assert!(configured.query.table_engine_memory_enabled);

            assert!(configured.query.table_cache_enabled);
            assert_eq!(10240, configured.query.table_cache_segment_count);
            assert_eq!(256, configured.query.table_cache_snapshot_count);
            assert_eq!(3000, configured.query.table_cache_bloom_index_meta_count);
            assert_eq!(
                1024 * 1024 * 1024,
                configured.query.table_cache_bloom_index_data_bytes
            );
            assert_eq!(HashMap::new(), configured.catalogs);
        },
    );

    Ok(())
}

// From env, defaulting.
#[test]
fn test_env_config_fs() -> Result<()> {
    temp_env::with_vars(
        vec![
            ("LOG_LEVEL", Some("DEBUG")),
            ("QUERY_TENANT_ID", Some("tenant-1")),
            ("QUERY_CLUSTER_ID", Some("cluster-1")),
            ("QUERY_MYSQL_HANDLER_HOST", Some("127.0.0.1")),
            ("QUERY_MYSQL_HANDLER_PORT", Some("3306")),
            ("QUERY_MAX_ACTIVE_SESSIONS", Some("255")),
            ("QUERY_CLICKHOUSE_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HANDLER_PORT", Some("9000")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_PORT", Some("8124")),
            ("QUERY_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_HTTP_HANDLER_PORT", Some("8001")),
            ("QUERY_FLIGHT_API_ADDRESS", Some("1.2.3.4:9091")),
            ("QUERY_ADMIN_API_ADDRESS", Some("1.2.3.4:8081")),
            ("QUERY_METRIC_API_ADDRESS", Some("1.2.3.4:7071")),
            ("QUERY_TABLE_CACHE_ENABLED", Some("true")),
            ("QUERY_TABLE_MEMORY_CACHE_MB_SIZE", Some("512")),
            ("QUERY_TABLE_DISK_CACHE_ROOT", Some("_cache_env")),
            ("QUERY_TABLE_DISK_CACHE_MB_SIZE", Some("512")),
            ("QU-ERY_TABLE_CACHE_SNAPSHOT_COUNT", Some("256")),
            ("QUERY_TABLE_CACHE_SEGMENT_COUNT", Some("10240")),
            ("META_ENDPOINTS", Some("0.0.0.0:9191")),
            ("TABLE_CACHE_BLOOM_INDEX_META_COUNT", Some("3000")),
            (
                "TABLE_CACHE_BLOOM_INDEX_DATA_BYTES",
                Some(format!("{}", 1024 * 1024 * 1024).as_str()),
            ),
            ("STORAGE_TYPE", Some("fs")),
            ("STORAGE_NUM_CPUS", Some("16")),
            ("STORAGE_FS_DATA_PATH", Some("/tmp/test")),
            ("STORAGE_S3_REGION", Some("us.region")),
            ("STORAGE_S3_ENDPOINT_URL", Some("http://127.0.0.1:10024")),
            ("STORAGE_S3_ACCESS_KEY_ID", Some("us.key.id")),
            ("STORAGE_S3_SECRET_ACCESS_KEY", Some("us.key")),
            ("STORAGE_S3_BUCKET", Some("us.bucket")),
            (
                "STORAGE_GCS_ENDPOINT_URL",
                Some("http://gcs.storage.cname_map.local"),
            ),
            ("STORAGE_GCS_BUCKET", Some("gcs.bucket")),
            ("STORAGE_GCS_ROOT", Some("/path/to/root")),
            ("STORAGE_GCS_CREDENTIAL", Some("gcs.credential")),
            ("STORAGE_OSS_BUCKET", Some("oss.bucket")),
            (
                "STORAGE_OSS_ENDPOINT_URL",
                Some("https://oss-cn-litang.example.com"),
            ),
            ("STORAGE_OSS_ROOT", Some("oss.root")),
            ("STORAGE_OSS_ACCESS_KEY_ID", Some("access_key_id")),
            ("STORAGE_OSS_ACCESS_KEY_SECRET", Some("access_key_secret")),
            ("QUERY_TABLE_ENGINE_MEMORY_ENABLED", Some("true")),
            ("CONFIG_FILE", None),
        ],
        || {
            let configured = Config::load_for_test().expect("must success").into_outer();

            assert_eq!("DEBUG", configured.log.level);

            assert_eq!("tenant-1", configured.query.tenant_id);
            assert_eq!("cluster-1", configured.query.cluster_id);
            assert_eq!("127.0.0.1", configured.query.mysql_handler_host);
            assert_eq!(3306, configured.query.mysql_handler_port);
            assert_eq!(255, configured.query.max_active_sessions);
            assert_eq!("1.2.3.4", configured.query.clickhouse_http_handler_host);
            assert_eq!(8124, configured.query.clickhouse_http_handler_port);
            assert_eq!("1.2.3.4", configured.query.http_handler_host);
            assert_eq!(8001, configured.query.http_handler_port);

            assert_eq!(1, configured.meta.endpoints.len());
            assert_eq!("0.0.0.0:9191", configured.meta.endpoints[0]);

            assert_eq!("1.2.3.4:9091", configured.query.flight_api_address);
            assert_eq!("1.2.3.4:8081", configured.query.admin_api_address);
            assert_eq!("1.2.3.4:7071", configured.query.metric_api_address);

            assert_eq!("fs", configured.storage.storage_type);
            assert_eq!(16, configured.storage.storage_num_cpus);

            assert_eq!("/tmp/test", configured.storage.fs.data_path);

            // Storage type is fs, s3 related value should be default.
            assert_eq!("", configured.storage.s3.region);
            assert_eq!(
                "https://s3.amazonaws.com",
                configured.storage.s3.endpoint_url
            );

            // Storage type is fs, gcs related value should be default.
            assert_eq!(
                "https://storage.googleapis.com",
                configured.storage.gcs.gcs_endpoint_url
            );
            assert_eq!("", configured.storage.gcs.gcs_bucket);
            assert_eq!("", configured.storage.gcs.gcs_root);
            assert_eq!("", configured.storage.gcs.credential);

            assert!(configured.query.table_engine_memory_enabled);

            assert!(configured.query.table_cache_enabled);
            assert_eq!(512, configured.query.table_memory_cache_mb_size);
            assert_eq!("_cache_env", configured.query.table_disk_cache_root);
            assert_eq!(512, configured.query.table_disk_cache_mb_size);
            assert_eq!(10240, configured.query.table_cache_segment_count);
            assert_eq!(256, configured.query.table_cache_snapshot_count);
            assert_eq!(3000, configured.query.table_cache_bloom_index_meta_count);
            assert_eq!(
                1024 * 1024 * 1024,
                configured.query.table_cache_bloom_index_data_bytes
            );
        },
    );

    Ok(())
}

#[test]
fn test_env_config_gcs() -> Result<()> {
    temp_env::with_vars(
        vec![
            ("LOG_LEVEL", Some("DEBUG")),
            ("QUERY_TENANT_ID", Some("tenant-1")),
            ("QUERY_CLUSTER_ID", Some("cluster-1")),
            ("QUERY_MYSQL_HANDLER_HOST", Some("127.0.0.1")),
            ("QUERY_MYSQL_HANDLER_PORT", Some("3306")),
            ("QUERY_MAX_ACTIVE_SESSIONS", Some("255")),
            ("QUERY_CLICKHOUSE_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HANDLER_PORT", Some("9000")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_PORT", Some("8124")),
            ("QUERY_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_HTTP_HANDLER_PORT", Some("8001")),
            ("QUERY_FLIGHT_API_ADDRESS", Some("1.2.3.4:9091")),
            ("QUERY_ADMIN_API_ADDRESS", Some("1.2.3.4:8081")),
            ("QUERY_METRIC_API_ADDRESS", Some("1.2.3.4:7071")),
            ("QUERY_TABLE_CACHE_ENABLED", Some("true")),
            ("QUERY_TABLE_MEMORY_CACHE_MB_SIZE", Some("512")),
            ("QUERY_TABLE_DISK_CACHE_ROOT", Some("_cache_env")),
            ("QUERY_TABLE_DISK_CACHE_MB_SIZE", Some("512")),
            ("QUERY_TABLE_CACHE_SNAPSHOT_COUNT", Some("256")),
            ("QUERY_TABLE_CACHE_SEGMENT_COUNT", Some("10240")),
            ("META_ENDPOINTS", Some("0.0.0.0:9191")),
            ("TABLE_CACHE_BLOOM_INDEX_META_COUNT", Some("3000")),
            (
                "TABLE_CACHE_BLOOM_INDEX_DATA_BYTES",
                Some(format!("{}", 1024 * 1024 * 1024).as_str()),
            ),
            ("STORAGE_TYPE", Some("gcs")),
            ("STORAGE_NUM_CPUS", Some("16")),
            ("STORAGE_FS_DATA_PATH", Some("/tmp/test")),
            ("STORAGE_S3_REGION", Some("us.region")),
            ("STORAGE_S3_ENDPOINT_URL", Some("http://127.0.0.1:10024")),
            ("STORAGE_S3_ACCESS_KEY_ID", Some("us.key.id")),
            ("STORAGE_S3_SECRET_ACCESS_KEY", Some("us.key")),
            ("STORAGE_S3_BUCKET", Some("us.bucket")),
            (
                "STORAGE_GCS_ENDPOINT_URL",
                Some("http://gcs.storage.cname_map.local"),
            ),
            ("STORAGE_GCS_BUCKET", Some("gcs.bucket")),
            ("STORAGE_GCS_ROOT", Some("/path/to/root")),
            ("STORAGE_GCS_CREDENTIAL", Some("gcs.credential")),
            ("STORAGE_OSS_BUCKET", Some("oss.bucket")),
            (
                "STORAGE_OSS_ENDPOINT_URL",
                Some("https://oss-cn-litang.example.com"),
            ),
            ("STORAGE_OSS_ROOT", Some("oss.root")),
            ("STORAGE_OSS_ACCESS_KEY_ID", Some("access_key_id")),
            ("STORAGE_OSS_ACCESS_KEY_SECRET", Some("access_key_secret")),
            ("QUERY_TABLE_ENGINE_MEMORY_ENABLED", Some("true")),
            ("CONFIG_FILE", None),
        ],
        || {
            let configured = Config::load_for_test().expect("must success").into_outer();

            assert_eq!("DEBUG", configured.log.level);

            assert_eq!("tenant-1", configured.query.tenant_id);
            assert_eq!("cluster-1", configured.query.cluster_id);
            assert_eq!("127.0.0.1", configured.query.mysql_handler_host);
            assert_eq!(3306, configured.query.mysql_handler_port);
            assert_eq!(255, configured.query.max_active_sessions);
            assert_eq!("1.2.3.4", configured.query.clickhouse_http_handler_host);
            assert_eq!(8124, configured.query.clickhouse_http_handler_port);
            assert_eq!("1.2.3.4", configured.query.http_handler_host);
            assert_eq!(8001, configured.query.http_handler_port);

            assert_eq!(1, configured.meta.endpoints.len());
            assert_eq!("0.0.0.0:9191", configured.meta.endpoints[0]);

            assert_eq!("1.2.3.4:9091", configured.query.flight_api_address);
            assert_eq!("1.2.3.4:8081", configured.query.admin_api_address);
            assert_eq!("1.2.3.4:7071", configured.query.metric_api_address);

            assert_eq!("gcs", configured.storage.storage_type);
            assert_eq!(16, configured.storage.storage_num_cpus);

            assert_eq!(
                "http://gcs.storage.cname_map.local",
                configured.storage.gcs.gcs_endpoint_url
            );
            assert_eq!("gcs.bucket", configured.storage.gcs.gcs_bucket);
            assert_eq!("/path/to/root", configured.storage.gcs.gcs_root);
            assert_eq!("gcs.credential", configured.storage.gcs.credential);

            // Storage type is gcs, fs related value should stay default.
            assert_eq!("_data", configured.storage.fs.data_path);

            // Storage type is gcs, s3 related value should be default.
            assert_eq!("", configured.storage.s3.region);
            assert_eq!(
                "https://s3.amazonaws.com",
                configured.storage.s3.endpoint_url
            );

            // Storage type is gcs, oss related value should be default.
            assert_eq!("", configured.storage.oss.oss_endpoint_url);
            assert_eq!("", configured.storage.oss.oss_bucket);
            assert_eq!("", configured.storage.oss.oss_root);
            assert_eq!("", configured.storage.oss.oss_access_key_id);
            assert_eq!("", configured.storage.oss.oss_access_key_secret);

            assert!(configured.query.table_engine_memory_enabled);

            assert!(configured.query.table_cache_enabled);
            assert_eq!(512, configured.query.table_memory_cache_mb_size);
            assert_eq!("_cache_env", configured.query.table_disk_cache_root);
            assert_eq!(512, configured.query.table_disk_cache_mb_size);
            assert_eq!(10240, configured.query.table_cache_segment_count);
            assert_eq!(256, configured.query.table_cache_snapshot_count);
            assert_eq!(3000, configured.query.table_cache_bloom_index_meta_count);
            assert_eq!(
                1024 * 1024 * 1024,
                configured.query.table_cache_bloom_index_data_bytes
            );
        },
    );

    Ok(())
}

#[test]
fn test_env_config_oss() -> Result<()> {
    temp_env::with_vars(
        vec![
            ("LOG_LEVEL", Some("DEBUG")),
            ("QUERY_TENANT_ID", Some("tenant-1")),
            ("QUERY_CLUSTER_ID", Some("cluster-1")),
            ("QUERY_MYSQL_HANDLER_HOST", Some("127.0.0.1")),
            ("QUERY_MYSQL_HANDLER_PORT", Some("3306")),
            ("QUERY_MAX_ACTIVE_SESSIONS", Some("255")),
            ("QUERY_CLICKHOUSE_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HANDLER_PORT", Some("9000")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_CLICKHOUSE_HTTP_HANDLER_PORT", Some("8124")),
            ("QUERY_HTTP_HANDLER_HOST", Some("1.2.3.4")),
            ("QUERY_HTTP_HANDLER_PORT", Some("8001")),
            ("QUERY_FLIGHT_API_ADDRESS", Some("1.2.3.4:9091")),
            ("QUERY_ADMIN_API_ADDRESS", Some("1.2.3.4:8081")),
            ("QUERY_METRIC_API_ADDRESS", Some("1.2.3.4:7071")),
            ("QUERY_TABLE_CACHE_ENABLED", Some("true")),
            ("QUERY_TABLE_MEMORY_CACHE_MB_SIZE", Some("512")),
            ("QUERY_TABLE_DISK_CACHE_ROOT", Some("_cache_env")),
            ("QUERY_TABLE_DISK_CACHE_MB_SIZE", Some("512")),
            ("QUERY_TABLE_CACHE_SNAPSHOT_COUNT", Some("256")),
            ("QUERY_TABLE_CACHE_SEGMENT_COUNT", Some("10240")),
            ("META_ENDPOINTS", Some("0.0.0.0:9191")),
            ("TABLE_CACHE_BLOOM_INDEX_META_COUNT", Some("3000")),
            (
                "TABLE_CACHE_BLOOM_INDEX_DATA_BYTES",
                Some(format!("{}", 1024 * 1024 * 1024).as_str()),
            ),
            ("STORAGE_TYPE", Some("oss")),
            ("STORAGE_NUM_CPUS", Some("16")),
            ("STORAGE_FS_DATA_PATH", Some("/tmp/test")),
            ("STORAGE_S3_REGION", Some("us.region")),
            ("STORAGE_S3_ENDPOINT_URL", Some("http://127.0.0.1:10024")),
            ("STORAGE_S3_ACCESS_KEY_ID", Some("us.key.id")),
            ("STORAGE_S3_SECRET_ACCESS_KEY", Some("us.key")),
            ("STORAGE_S3_BUCKET", Some("us.bucket")),
            (
                "STORAGE_GCS_ENDPOINT_URL",
                Some("http://gcs.storage.cname_map.local"),
            ),
            ("STORAGE_GCS_BUCKET", Some("gcs.bucket")),
            ("STORAGE_GCS_ROOT", Some("/path/to/root")),
            ("STORAGE_GCS_CREDENTIAL", Some("gcs.credential")),
            ("STORAGE_OSS_BUCKET", Some("oss.bucket")),
            (
                "STORAGE_OSS_ENDPOINT_URL",
                Some("https://oss-cn-litang.example.com"),
            ),
            ("STORAGE_OSS_ROOT", Some("oss.root")),
            ("STORAGE_OSS_ACCESS_KEY_ID", Some("access_key_id")),
            ("STORAGE_OSS_ACCESS_KEY_SECRET", Some("access_key_secret")),
            ("QUERY_TABLE_ENGINE_MEMORY_ENABLED", Some("true")),
            ("CONFIG_FILE", None),
        ],
        || {
            let configured = Config::load_for_test().expect("must success").into_outer();

            assert_eq!("DEBUG", configured.log.level);

            assert_eq!("tenant-1", configured.query.tenant_id);
            assert_eq!("cluster-1", configured.query.cluster_id);
            assert_eq!("127.0.0.1", configured.query.mysql_handler_host);
            assert_eq!(3306, configured.query.mysql_handler_port);
            assert_eq!(255, configured.query.max_active_sessions);
            assert_eq!("1.2.3.4", configured.query.clickhouse_http_handler_host);
            assert_eq!(8124, configured.query.clickhouse_http_handler_port);
            assert_eq!("1.2.3.4", configured.query.http_handler_host);
            assert_eq!(8001, configured.query.http_handler_port);

            assert_eq!("1.2.3.4:9091", configured.query.flight_api_address);
            assert_eq!("1.2.3.4:8081", configured.query.admin_api_address);
            assert_eq!("1.2.3.4:7071", configured.query.metric_api_address);

            assert_eq!(1, configured.meta.endpoints.len());
            assert_eq!("0.0.0.0:9191", configured.meta.endpoints[0]);

            assert_eq!("oss", configured.storage.storage_type);
            assert_eq!(16, configured.storage.storage_num_cpus);

            // Storage type is oss, s3 related value should be default.
            assert_eq!("", configured.storage.s3.region);
            assert_eq!(
                "https://s3.amazonaws.com",
                configured.storage.s3.endpoint_url
            );

            // config of fs should not be loaded, take default value.
            assert_eq!("_data", configured.storage.fs.data_path);

            // Storage type is oss, gcs related value should be default.
            assert_eq!(
                "https://storage.googleapis.com",
                configured.storage.gcs.gcs_endpoint_url
            );

            assert_eq!(
                "https://oss-cn-litang.example.com",
                configured.storage.oss.oss_endpoint_url
            );
            assert_eq!("oss.bucket", configured.storage.oss.oss_bucket);
            assert_eq!("oss.root", configured.storage.oss.oss_root);
            assert_eq!("access_key_id", configured.storage.oss.oss_access_key_id);
            assert_eq!(
                "access_key_secret",
                configured.storage.oss.oss_access_key_secret
            );

            assert_eq!("", configured.storage.gcs.gcs_bucket);
            assert_eq!("", configured.storage.gcs.gcs_root);
            assert_eq!("", configured.storage.gcs.credential);

            assert!(configured.query.table_engine_memory_enabled);

            assert!(configured.query.table_cache_enabled);
            assert_eq!(512, configured.query.table_memory_cache_mb_size);
            assert_eq!("_cache_env", configured.query.table_disk_cache_root);
            assert_eq!(512, configured.query.table_disk_cache_mb_size);
            assert_eq!(10240, configured.query.table_cache_segment_count);
            assert_eq!(256, configured.query.table_cache_snapshot_count);
            assert_eq!(3000, configured.query.table_cache_bloom_index_meta_count);
            assert_eq!(
                1024 * 1024 * 1024,
                configured.query.table_cache_bloom_index_data_bytes
            );
        },
    );
    Ok(())
}

/// Test whether override works as expected.
#[test]
fn test_override_config() -> Result<()> {
    let file_path = temp_dir().join("databend_test_config.toml");

    let mut f = fs::File::create(&file_path)?;
    f.write_all(
        r#"config_file = ""

[query]
tenant_id = "tenant_id_from_file"
cluster_id = ""
num_cpus = 0
mysql_handler_host = "127.0.0.1"
mysql_handler_port = 3307
max_active_sessions = 256
max_server_memory_usage = 0
clickhouse_handler_host = "127.0.0.1"
clickhouse_handler_port = 9000
clickhouse_http_handler_host = "127.0.0.1"
clickhouse_http_handler_port = 8124
http_handler_host = "127.0.0.1"
http_handler_port = 8000
http_handler_result_timeout_secs = 60
flight_api_address = "127.0.0.1:9090"
admin_api_address = "127.0.0.1:8080"
metric_api_address = "127.0.0.1:7070"
http_handler_tls_server_cert = ""
http_handler_tls_server_key = ""
http_handler_tls_server_root_ca_cert = ""
api_tls_server_cert = ""
api_tls_server_key = ""
api_tls_server_root_ca_cert = ""
rpc_tls_server_cert = ""
rpc_tls_server_key = ""
rpc_tls_query_server_root_ca_cert = ""
rpc_tls_query_service_domain_name = "localhost"
table_engine_memory_enabled = true
database_engine_github_enabled = true
wait_timeout_mills = 5000
max_query_log_size = 10000
table_cache_enabled = false
table_cache_snapshot_count = 256
table_cache_segment_count = 10240
table_cache_block_meta_count = 102400
table_memory_cache_mb_size = 256
table_disk_cache_root = "_cache"
table_disk_cache_mb_size = 1024
table_cache_bloom_index_meta_count = 3000
table_cache_bloom_index_data_bytes = 1073741824
management_mode = false
jwt_key_file = ""
async_insert_max_data_size = 10000
async_insert_busy_timeout = 200
async_insert_stale_timeout = 0
users = []
share_endpoint_address = ""

[log]
level = "INFO"
dir = "./.databend/logs"
query_enabled = false

[meta]
endpoints = ["0.0.0.0:9191"]
username = "username_from_file"
password = "password_from_file"
client_timeout_in_second = 10
rpc_tls_meta_server_root_ca_cert = ""
rpc_tls_meta_service_domain_name = "localhost"

[storage]
type = "s3"
num_cpus = 0

[storage.fs]
data_path = "./.datebend/data"

[storage.s3]
region = ""
endpoint_url = "https://s3.amazonaws.com"
access_key_id = "access_key_id_from_file"
secret_access_key = ""
bucket = ""
root = ""
master_key = ""

[storage.azblob]
account_name = ""
account_key = ""
container = ""
endpoint_url = ""
root = ""

[storage.hdfs]
name_node = ""
root = ""

[storage.obs]
endpoint_url = ""
access_key_id = ""
secret_access_key = ""
bucket = ""
root = ""

[storage.oss]
endpoint_url = ""
access_key_id = ""
access_key_secret = ""
bucket = ""
root = ""

[catalog]
address = "127.0.0.1:9083"
protocol = "binary"

[catalogs.my_hive]
type = "hive"
address = "127.0.0.1:9083"
protocol = "binary"
"#
        .as_bytes(),
    )?;

    // Make sure all data flushed.
    f.flush()?;

    temp_env::with_vars(
        vec![
            ("CONFIG_FILE", Some(file_path.to_string_lossy().as_ref())),
            ("QUERY_TENANT_ID", Some("tenant_id_from_env")),
            ("STORAGE_S3_ACCESS_KEY_ID", Some("access_key_id_from_env")),
            ("STORAGE_TYPE", None),
        ],
        || {
            let cfg = Config::load_for_test()
                .expect("config load success")
                .into_outer();

            assert_eq!("tenant_id_from_env", cfg.query.tenant_id);
            assert_eq!("access_key_id_from_env", cfg.storage.s3.access_key_id);
            assert_eq!("s3", cfg.storage.storage_type);

            // NOTE:
            //
            // after the config conversion procedure:
            // Outer -> Inner -> Outer
            //
            // config in `catalog` field will be moved to `catalogs` field
            assert!(cfg.catalog.meta_store_address.is_empty());
            assert!(cfg.catalog.protocol.is_empty());
            // config in `catalog` field, with name of "hive"
            assert!(cfg.catalogs.get("hive").is_some(), "catalogs is none!");
            // config in `catalogs` field, with name of "my_hive"
            assert!(cfg.catalogs.get("my_hive").is_some(), "catalogs is none!");

            let inner = cfg.catalogs["my_hive"].clone().try_into();
            assert!(inner.is_ok(), "casting must success");
            let cfg = inner.unwrap();
            match cfg {
                CatalogConfig::Hive(cfg) => {
                    assert_eq!("127.0.0.1:9083", cfg.address, "address incorrect");
                    assert_eq!("binary", cfg.protocol.to_string(), "protocol incorrect");
                }
            }
        },
    );

    // remove temp file
    fs::remove_file(file_path)?;

    Ok(())
}

/// Test old hive catalog
#[test]
fn test_override_config_old_hive_catalog() -> Result<()> {
    let file_path = temp_dir().join("databend_test_override_config_old_hive_catalog.toml");

    let mut f = fs::File::create(&file_path)?;
    f.write_all(
        r#"
[catalog]
meta_store_address = "1.1.1.1:10000"
protocol = "binary"
"#
        .as_bytes(),
    )?;

    // Make sure all data flushed.
    f.flush()?;

    temp_env::with_vars(
        vec![("CONFIG_FILE", Some(file_path.to_string_lossy().as_ref()))],
        || {
            let cfg = Config::load_for_test().expect("config load success");

            assert_eq!(
                cfg.catalogs["hive"],
                CatalogConfig::Hive(CatalogHiveConfig {
                    address: "1.1.1.1:10000".to_string(),
                    protocol: ThriftProtocol::Binary,
                })
            );
        },
    );

    // remove temp file
    fs::remove_file(file_path)?;

    Ok(())
}

/// Test new hive catalog
#[test]
fn test_override_config_new_hive_catalog() -> Result<()> {
    let file_path = temp_dir().join("databend_test_override_config_new_hive_catalog.toml");

    let mut f = fs::File::create(&file_path)?;
    f.write_all(
        r#"
[catalogs.my_hive]
type = "hive"
address = "1.1.1.1:12000"
protocol = "binary"
"#
        .as_bytes(),
    )?;

    // Make sure all data flushed.
    f.flush()?;

    temp_env::with_vars(
        vec![("CONFIG_FILE", Some(file_path.to_string_lossy().as_ref()))],
        || {
            let cfg = Config::load_for_test().expect("config load success");

            assert_eq!(
                cfg.catalogs["my_hive"],
                CatalogConfig::Hive(CatalogHiveConfig {
                    address: "1.1.1.1:12000".to_string(),
                    protocol: ThriftProtocol::Binary,
                })
            );
        },
    );

    // remove temp file
    fs::remove_file(file_path)?;

    Ok(())
}
