// Copyright 2022 Datafuse Labs.
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

use std::env;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::ops::Deref;
use std::time::Duration;

use anyhow::anyhow;
use backon::ExponentialBackoff;
use common_base::base::GlobalIORuntime;
use common_base::base::Singleton;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use once_cell::sync::OnceCell;
use opendal::layers::ImmutableIndexLayer;
use opendal::layers::LoggingLayer;
use opendal::layers::MetricsLayer;
use opendal::layers::RetryLayer;
use opendal::layers::TracingLayer;
use opendal::services::azblob;
use opendal::services::fs;
use opendal::services::ftp;
use opendal::services::gcs;
use opendal::services::http;
use opendal::services::memory;
use opendal::services::moka;
use opendal::services::obs;
use opendal::services::oss;
use opendal::services::redis;
use opendal::services::s3;
use opendal::Operator;

use super::StorageAzblobConfig;
use super::StorageFsConfig;
use super::StorageParams;
use super::StorageS3Config;
use crate::config::StorageGcsConfig;
use crate::config::StorageHttpConfig;
use crate::config::StorageMokaConfig;
use crate::config::StorageObsConfig;
use crate::runtime_layer::RuntimeLayer;
use crate::CacheConfig;
use crate::StorageConfig;
use crate::StorageOssConfig;
use crate::StorageRedisConfig;

/// init_operator will init an opendal operator based on storage config.
pub fn init_operator(cfg: &StorageParams) -> Result<Operator> {
    let op = match &cfg {
        StorageParams::Azblob(cfg) => init_azblob_operator(cfg)?,
        StorageParams::Fs(cfg) => init_fs_operator(cfg)?,
        StorageParams::Ftp(cfg) => init_ftp_operator(cfg)?,
        StorageParams::Gcs(cfg) => init_gcs_operator(cfg)?,
        #[cfg(feature = "storage-hdfs")]
        StorageParams::Hdfs(cfg) => init_hdfs_operator(cfg)?,
        StorageParams::Http(cfg) => init_http_operator(cfg)?,
        StorageParams::Ipfs(cfg) => init_ipfs_operator(cfg)?,
        StorageParams::Memory => init_memory_operator()?,
        StorageParams::Moka(cfg) => init_moka_operator(cfg)?,
        StorageParams::Obs(cfg) => init_obs_operator(cfg)?,
        StorageParams::S3(cfg) => init_s3_operator(cfg)?,
        StorageParams::Oss(cfg) => init_oss_operator(cfg)?,
        StorageParams::Redis(cfg) => init_redis_operator(cfg)?,
        v => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!("Unsupported storage type: {:?}", v),
            ));
        }
    };

    let op = op
        // Add retry
        .layer(RetryLayer::new(ExponentialBackoff::default().with_jitter()))
        // Add metrics
        .layer(MetricsLayer)
        // Add logging
        .layer(LoggingLayer)
        // Add tracing
        .layer(TracingLayer)
        // NOTE
        //
        // Magic happens here. We will add a layer upon original
        // storage operator so that all underlying storage operations
        // will send to storage runtime.
        .layer(RuntimeLayer::new(GlobalIORuntime::instance().inner()));

    Ok(op)
}

/// init_azblob_operator will init an opendal azblob operator.
pub fn init_azblob_operator(cfg: &StorageAzblobConfig) -> Result<Operator> {
    let mut builder = azblob::Builder::default();

    // Endpoint
    builder.endpoint(&cfg.endpoint_url);

    // Container
    builder.container(&cfg.container);

    // Root
    builder.root(&cfg.root);

    // Credential
    builder.account_name(&cfg.account_name);
    builder.account_key(&cfg.account_key);

    Ok(Operator::new(builder.build()?))
}

/// init_fs_operator will init a opendal fs operator.
fn init_fs_operator(cfg: &StorageFsConfig) -> Result<Operator> {
    let mut builder = fs::Builder::default();

    let mut path = cfg.root.clone();
    if !path.starts_with('/') {
        path = env::current_dir().unwrap().join(path).display().to_string();
    }
    builder.root(&path);

    Ok(Operator::new(builder.build()?))
}

/// init_ftp_operator will init a opendal ftp operator.
fn init_ftp_operator(cfg: &super::StorageFtpConfig) -> Result<Operator> {
    let mut builder = ftp::Builder::default();

    let bd = builder
        .endpoint(&cfg.endpoint)
        .user(&cfg.username)
        .password(&cfg.password)
        .root(&cfg.root)
        .build()?;
    Ok(Operator::new(bd))
}

/// init_gcs_operator will init a opendal gcs operator.
fn init_gcs_operator(cfg: &StorageGcsConfig) -> Result<Operator> {
    let mut builder = gcs::Builder::default();

    let accessor = builder
        .endpoint(&cfg.endpoint_url)
        .bucket(&cfg.bucket)
        .root(&cfg.root)
        .credential(&cfg.credential)
        .build()?;

    Ok(Operator::new(accessor))
}

/// init_hdfs_operator will init an opendal hdfs operator.
#[cfg(feature = "storage-hdfs")]
fn init_hdfs_operator(cfg: &super::StorageHdfsConfig) -> Result<Operator> {
    use opendal::services::hdfs;

    let mut builder = hdfs::Builder::default();

    // Endpoint.
    builder.name_node(&cfg.name_node);

    // Root
    builder.root(&cfg.root);

    Ok(Operator::new(builder.build()?))
}

fn init_ipfs_operator(cfg: &super::StorageIpfsConfig) -> Result<Operator> {
    use opendal::services::ipfs;

    let mut builder = ipfs::Builder::default();

    builder.root(&cfg.root);
    builder.endpoint(&cfg.endpoint_url);

    Ok(Operator::new(builder.build()?))
}

fn init_http_operator(cfg: &StorageHttpConfig) -> Result<Operator> {
    let mut builder = http::Builder::default();

    // Endpoint.
    builder.endpoint(&cfg.endpoint_url);

    // HTTP Service is read-only and doesn't support list operation.
    // ImmutableIndexLayer will build an in-memory immutable index for it.
    let mut immutable_layer = ImmutableIndexLayer::default();
    let files: Vec<String> = cfg
        .paths
        .iter()
        .map(|v| v.trim_start_matches('/').to_string())
        .collect();
    // TODO: should be replace by `immutable_layer.extend_iter()` after fix
    for i in files {
        immutable_layer.insert(i);
    }

    Ok(Operator::new(builder.build()?).layer(immutable_layer))
}

/// init_memory_operator will init a opendal memory operator.
fn init_memory_operator() -> Result<Operator> {
    let mut builder = memory::Builder::default();

    Ok(Operator::new(builder.build()?))
}

/// init_s3_operator will init a opendal s3 operator with input s3 config.
fn init_s3_operator(cfg: &StorageS3Config) -> Result<Operator> {
    let mut builder = s3::Builder::default();

    // Endpoint.
    builder.endpoint(&cfg.endpoint_url);

    // Region
    builder.region(&cfg.region);

    // Credential.
    builder.access_key_id(&cfg.access_key_id);
    builder.secret_access_key(&cfg.secret_access_key);
    builder.security_token(&cfg.security_token);
    builder.role_arn(&cfg.role_arn);
    builder.external_id(&cfg.external_id);

    // Bucket.
    builder.bucket(&cfg.bucket);

    // Root.
    builder.root(&cfg.root);

    // Disable credential loader
    if cfg.disable_credential_loader {
        builder.disable_credential_loader();
    }

    // Enable virtual host style
    if cfg.enable_virtual_host_style {
        builder.enable_virtual_host_style();
    }

    Ok(Operator::new(builder.build()?))
}

/// init_obs_operator will init a opendal obs operator with input obs config.
fn init_obs_operator(cfg: &StorageObsConfig) -> Result<Operator> {
    let mut builder = obs::Builder::default();
    // Endpoint
    builder.endpoint(&cfg.endpoint_url);
    // Bucket
    builder.bucket(&cfg.bucket);
    // Root
    builder.root(&cfg.root);
    // Credential
    builder.access_key_id(&cfg.access_key_id);
    builder.secret_access_key(&cfg.secret_access_key);

    Ok(Operator::new(builder.build()?))
}

/// init_oss_operator will init an opendal OSS operator with input oss config.
fn init_oss_operator(cfg: &StorageOssConfig) -> Result<Operator> {
    let mut builder = oss::Builder::default();

    // endpoint
    let backend = builder
        .endpoint(&cfg.endpoint_url)
        .access_key_id(&cfg.access_key_id)
        .access_key_secret(&cfg.access_key_secret)
        .bucket(&cfg.bucket)
        .root(&cfg.root)
        .build()?;

    Ok(Operator::new(backend))
}

/// init_moka_operator will init a moka operator.
fn init_moka_operator(v: &StorageMokaConfig) -> Result<Operator> {
    let mut builder = moka::Builder::default();

    builder.max_capacity(v.max_capacity);
    builder.time_to_live(std::time::Duration::from_secs(v.time_to_live as u64));
    builder.time_to_idle(std::time::Duration::from_secs(v.time_to_idle as u64));

    Ok(Operator::new(builder.build()?))
}

/// init_redis_operator will init a reids operator.
fn init_redis_operator(v: &StorageRedisConfig) -> Result<Operator> {
    let mut builder = redis::Builder::default();

    builder.endpoint(&v.endpoint_url);
    builder.root(&v.root);
    builder.db(v.db);
    if let Some(v) = v.default_ttl {
        builder.default_ttl(Duration::from_secs(v as u64));
    }
    if let Some(v) = &v.username {
        builder.username(v);
    }
    if let Some(v) = &v.password {
        builder.password(v);
    }

    Ok(Operator::new(builder.build()?))
}

/// PersistOperator is the operator to access persist services.
///
/// # Notes
///
/// All data accessed via this operator will be persisted.
#[derive(Clone, Debug)]
pub struct DataOperator {
    operator: Operator,
    params: StorageParams,
}

impl Deref for DataOperator {
    type Target = Operator;

    fn deref(&self) -> &Self::Target {
        &self.operator
    }
}

static DATA_OPERATOR: OnceCell<Singleton<DataOperator>> = OnceCell::new();

impl DataOperator {
    /// Create a new persist operator.
    pub fn new(op: Operator, params: StorageParams) -> Self {
        Self {
            operator: op,
            params,
        }
    }

    /// Get the operator from PersistOperator
    pub fn operator(&self) -> Operator {
        self.operator.clone()
    }

    /// Get the params from PersistOperator
    pub fn params(&self) -> &StorageParams {
        &self.params
    }

    pub async fn init(
        conf: &StorageConfig,
        v: Singleton<DataOperator>,
    ) -> common_exception::Result<()> {
        v.init(Self::try_create(conf).await?)?;

        DATA_OPERATOR.set(v).ok();
        Ok(())
    }

    pub async fn try_create(conf: &StorageConfig) -> common_exception::Result<DataOperator> {
        Self::try_create_with_storage_params(&conf.params).await
    }

    pub async fn try_create_with_storage_params(
        sp: &StorageParams,
    ) -> common_exception::Result<DataOperator> {
        let operator = init_operator(sp)?;

        // OpenDAL will send a real request to underlying storage to check whether it works or not.
        // If this check failed, it's highly possible that the users have configured it wrongly.
        //
        // Make sure the check is called inside GlobalIORuntime to prevent
        // IO hang on reuse connection.
        let op = operator.clone();
        if let Err(cause) = GlobalIORuntime::instance()
            .spawn(async move { op.check().await })
            .await
            .expect("join must succeed")
        {
            return Err(ErrorCode::StorageUnavailable(format!(
                "current configured storage is not available: config: {:?}, cause: {cause}",
                sp
            )));
        }

        Ok(DataOperator {
            operator,
            params: sp.clone(),
        })
    }

    pub fn instance() -> DataOperator {
        match DATA_OPERATOR.get() {
            None => panic!("StorageOperator is not init"),
            Some(storage_operator) => storage_operator.get(),
        }
    }

    pub fn get_storage_params(&self) -> StorageParams {
        self.params.clone()
    }
}

/// CacheOperator is the operator to access cache services.
///
/// # Notes
///
/// As described in [RFC: Cache](https://databend.rs/doc/contributing/rfcs/cache):
///
/// All data stored in cache operator should be non-persist and could be GC or
/// background auto evict at any time.
#[derive(Clone, Debug)]
pub struct CacheOperator {
    op: Option<Operator>,
}

static CACHE_OPERATOR: OnceCell<Singleton<CacheOperator>> = OnceCell::new();

impl CacheOperator {
    pub async fn init(
        conf: &CacheConfig,
        v: Singleton<CacheOperator>,
    ) -> common_exception::Result<()> {
        v.init(Self::try_create(conf).await?)?;

        CACHE_OPERATOR.set(v).ok();
        Ok(())
    }

    pub async fn try_create(conf: &CacheConfig) -> common_exception::Result<CacheOperator> {
        if conf.params == StorageParams::None {
            return Ok(CacheOperator { op: None });
        }

        let operator = init_operator(&conf.params)?;

        // OpenDAL will send a real request to underlying storage to check whether it works or not.
        // If this check failed, it's highly possible that the users have configured it wrongly.
        //
        // Make sure the check is called inside GlobalIORuntime to prevent
        // IO hang on reuse connection.
        let op = operator.clone();
        if let Err(cause) = GlobalIORuntime::instance()
            .spawn(async move { op.object("health_check").create().await })
            .await
            .expect("join must succeed")
        {
            return Err(ErrorCode::StorageUnavailable(format!(
                "current configured cache is not available: config: {:?}, cause: {cause}",
                conf
            )));
        }

        Ok(CacheOperator { op: Some(operator) })
    }

    pub fn instance() -> Option<Operator> {
        match CACHE_OPERATOR.get() {
            None => panic!("CacheOperator is not init"),
            Some(op) => op.get().inner(),
        }
    }

    fn inner(&self) -> Option<Operator> {
        self.op.clone()
    }
}
