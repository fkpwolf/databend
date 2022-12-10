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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_base::base::GlobalIORuntime;
use common_base::base::Runtime;
use common_base::base::SingletonImpl;
use common_catalog::catalog::CatalogManager;
use common_config::Config;
use common_config::GlobalConfig;
use common_exception::Result;
use common_storage::CacheOperator;
use common_storage::DataOperator;
use common_storages_table_meta::caches::CacheManager;
use common_tracing::set_panic_hook;
use common_tracing::QueryLogger;
use common_users::RoleCacheManager;
use common_users::UserApiProvider;
use databend_query::api::DataExchangeManager;
use databend_query::catalogs::CatalogManagerHelper;
use databend_query::clusters::ClusterDiscovery;
use databend_query::servers::http::v1::HttpQueryManager;
use databend_query::sessions::SessionManager;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use time::Instant;

pub struct TestGuard {
    thread_name: String,
    services: Arc<TestGlobalServices>,
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        // Hack: The session may be referenced by other threads. Let's try to wait.
        let now = Instant::now();
        while !SessionManager::instance().processes_info().is_empty() {
            std::thread::sleep(Duration::from_millis(500));

            if now.elapsed() > Duration::from_secs(3) {
                break;
            }
        }

        self.services.remove_services(&self.thread_name);
    }
}

/// Hard code, in order to make each test share the global service instance, we made some hack code
///   - We use thread names as key to store global service instances, because rust test passes the test name through the thread name
///   - In the debug version, we enable the transfer of thread names by environment variables.
#[derive(Default)]
pub struct TestGlobalServices {
    query_config: Mutex<HashMap<String, Arc<Config>>>,
    global_runtime: Mutex<HashMap<String, Arc<Runtime>>>,
    query_logger: Mutex<HashMap<String, Arc<QueryLogger>>>,
    cluster_discovery: Mutex<HashMap<String, Arc<ClusterDiscovery>>>,
    storage_operator: Mutex<HashMap<String, DataOperator>>,
    cache_operator: Mutex<HashMap<String, CacheOperator>>,
    cache_manager: Mutex<HashMap<String, Arc<CacheManager>>>,
    catalog_manager: Mutex<HashMap<String, Arc<CatalogManager>>>,
    http_query_manager: Mutex<HashMap<String, Arc<HttpQueryManager>>>,
    data_exchange_manager: Mutex<HashMap<String, Arc<DataExchangeManager>>>,
    session_manager: Mutex<HashMap<String, Arc<SessionManager>>>,
    users_manager: Mutex<HashMap<String, Arc<UserApiProvider>>>,
    users_role_manager: Mutex<HashMap<String, Arc<RoleCacheManager>>>,
}

unsafe impl Send for TestGlobalServices {}

unsafe impl Sync for TestGlobalServices {}

static GLOBAL: OnceCell<Arc<TestGlobalServices>> = OnceCell::new();

impl TestGlobalServices {
    pub async fn setup(config: Config) -> Result<TestGuard> {
        set_panic_hook();
        std::env::set_var("UNIT_TEST", "TRUE");
        let global_services = GLOBAL.get_or_init(|| Arc::new(TestGlobalServices::default()));

        // The order of initialization is very important
        GlobalConfig::init(config.clone(), global_services.clone())?;

        let app_name_shuffle = format!("{}-{}", config.query.tenant_id, config.query.cluster_id);

        QueryLogger::init(app_name_shuffle, &config.log, global_services.clone())?;
        GlobalIORuntime::init(config.query.num_cpus as usize, global_services.clone())?;

        // Cluster discovery.
        ClusterDiscovery::init(config.clone(), global_services.clone()).await?;

        DataOperator::init(&config.storage, global_services.clone()).await?;
        CacheOperator::init(&config.storage.cache, global_services.clone()).await?;
        CacheManager::init(&config.query, global_services.clone())?;
        CatalogManager::init(&config, global_services.clone()).await?;
        HttpQueryManager::init(&config, global_services.clone()).await?;
        DataExchangeManager::init(global_services.clone())?;
        SessionManager::init(&config, global_services.clone())?;
        UserApiProvider::init(
            config.meta.to_meta_grpc_client_conf(),
            config.query.idm.clone(),
            config.query.tenant_id.clone().as_str(),
            config.query.tenant_quota.clone(),
            global_services.clone(),
        )
        .await?;
        RoleCacheManager::init(global_services.clone())?;

        ClusterDiscovery::instance()
            .register_to_metastore(&config)
            .await?;

        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(thread_name) => Ok(TestGuard {
                services: global_services.clone(),
                thread_name: thread_name.to_string(),
            }),
        }
    }

    pub fn remove_services(&self, key: &str) {
        {
            let mut global_config_guard = self.query_config.lock();
            let global_config = global_config_guard.remove(key);
            drop(global_config_guard);
            drop(global_config);
        }
        {
            let mut global_runtime_guard = self.global_runtime.lock();
            let global_runtime = global_runtime_guard.remove(key);
            drop(global_runtime_guard);
            drop(global_runtime);
        }
        {
            let mut query_logger_guard = self.query_logger.lock();
            let query_logger = query_logger_guard.remove(key);
            drop(query_logger_guard);
            drop(query_logger);
        }
        {
            let mut cluster_discovery_guard = self.cluster_discovery.lock();
            let cluster_discovery = cluster_discovery_guard.remove(key);
            drop(cluster_discovery_guard);
            drop(cluster_discovery);
        }
        {
            let mut storage_operator_guard = self.storage_operator.lock();
            let storage_operator = storage_operator_guard.remove(key);
            drop(storage_operator_guard);
            drop(storage_operator);
        }
        {
            let mut cache_operator_guard = self.cache_operator.lock();
            let cache_operator = cache_operator_guard.remove(key);
            drop(cache_operator_guard);
            drop(cache_operator);
        }
        {
            let mut cache_manager_guard = self.cache_manager.lock();
            let cache_manager = cache_manager_guard.remove(key);
            drop(cache_manager_guard);
            drop(cache_manager);
        }
        {
            let mut catalog_manager_guard = self.catalog_manager.lock();
            let catalog_manager = catalog_manager_guard.remove(key);
            drop(catalog_manager_guard);
            drop(catalog_manager);
        }
        {
            let mut http_query_manager_guard = self.http_query_manager.lock();
            let http_query_manager = http_query_manager_guard.remove(key);
            drop(http_query_manager_guard);
            drop(http_query_manager);
        }
        {
            let mut data_exchange_manager_guard = self.data_exchange_manager.lock();
            let data_exchange_manager = data_exchange_manager_guard.remove(key);
            drop(data_exchange_manager_guard);
            drop(data_exchange_manager);
        }
        {
            let mut users_role_manager_guard = self.users_role_manager.lock();
            let users_role_manager = users_role_manager_guard.remove(key);
            drop(users_role_manager_guard);
            drop(users_role_manager);
        }
        {
            let mut users_manager_guard = self.users_manager.lock();
            let users_manager = users_manager_guard.remove(key);
            drop(users_manager_guard);
            drop(users_manager);
        }
        {
            let mut session_manager_guard = self.session_manager.lock();
            let session_manager = session_manager_guard.remove(key);
            drop(session_manager_guard);
            drop(session_manager);
        }
    }
}

impl SingletonImpl<Arc<Runtime>> for TestGlobalServices {
    fn get(&self) -> Arc<Runtime> {
        match std::thread::current().name() {
            None => panic!("Global runtime is not init"),
            Some(name) => match self.global_runtime.lock().get(name) {
                None => panic!("Global runtime is not init, while in test '{}'", name),
                Some(global_runtime) => global_runtime.clone(),
            },
        }
    }

    fn init(&self, value: Arc<Runtime>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.global_runtime.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("Global runtime set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<Arc<QueryLogger>> for TestGlobalServices {
    fn get(&self) -> Arc<QueryLogger> {
        match std::thread::current().name() {
            None => panic!("QueryLogger is not init"),
            Some(name) => match self.query_logger.lock().get(name) {
                None => panic!("QueryLogger is not init, while in test '{}'", name),
                Some(query_logger) => query_logger.clone(),
            },
        }
    }

    fn init(&self, value: Arc<QueryLogger>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.query_logger.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("QueryLogger set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<Arc<ClusterDiscovery>> for TestGlobalServices {
    fn get(&self) -> Arc<ClusterDiscovery> {
        match std::thread::current().name() {
            None => panic!("ClusterDiscovery is not init"),
            Some(name) => match self.cluster_discovery.lock().get(name) {
                None => panic!("ClusterDiscovery is not init, while in test '{}'", name),
                Some(cluster_discovery) => cluster_discovery.clone(),
            },
        }
    }

    fn init(&self, value: Arc<ClusterDiscovery>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.cluster_discovery.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("ClusterDiscovery set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<DataOperator> for TestGlobalServices {
    fn get(&self) -> DataOperator {
        match std::thread::current().name() {
            None => panic!("Operator is not init"),
            Some(name) => match self.storage_operator.lock().get(name) {
                None => panic!("Operator is not init, while in test '{}'", name),
                Some(storage_operator) => storage_operator.clone(),
            },
        }
    }

    fn init(&self, value: DataOperator) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.storage_operator.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("StorageOperator set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<CacheOperator> for TestGlobalServices {
    fn get(&self) -> CacheOperator {
        match std::thread::current().name() {
            None => panic!("CacheOperator is not init"),
            Some(name) => match self.cache_operator.lock().get(name) {
                None => panic!("CacheOperator is not init, while in test '{}'", name),
                Some(cache_operator) => cache_operator.clone(),
            },
        }
    }

    fn init(&self, value: CacheOperator) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.cache_operator.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("CacheOperator set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<Arc<CacheManager>> for TestGlobalServices {
    fn get(&self) -> Arc<CacheManager> {
        match std::thread::current().name() {
            None => panic!("CacheManager is not init"),
            Some(name) => match self.cache_manager.lock().get(name) {
                None => panic!("CacheManager is not init, while in test '{}'", name),
                Some(cache_manager) => cache_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<CacheManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.cache_manager.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("CacheManager set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<Arc<CatalogManager>> for TestGlobalServices {
    fn get(&self) -> Arc<CatalogManager> {
        match std::thread::current().name() {
            None => panic!("CatalogManager is not init"),
            Some(name) => match self.catalog_manager.lock().get(name) {
                None => panic!("CatalogManager is not init, while in test '{}'", name),
                Some(catalog_manager) => catalog_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<CatalogManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.catalog_manager.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("CatalogManager set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<Arc<HttpQueryManager>> for TestGlobalServices {
    fn get(&self) -> Arc<HttpQueryManager> {
        match std::thread::current().name() {
            None => panic!("HttpQueryManager is not init"),
            Some(name) => match self.http_query_manager.lock().get(name) {
                None => panic!("HttpQueryManager is not init, while in test '{}'", name),
                Some(http_query_manager) => http_query_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<HttpQueryManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.http_query_manager.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("HttpQueryManager set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<Arc<DataExchangeManager>> for TestGlobalServices {
    fn get(&self) -> Arc<DataExchangeManager> {
        match std::thread::current().name() {
            None => panic!("DataExchangeManager is not init"),
            Some(name) => match self.data_exchange_manager.lock().get(name) {
                None => panic!("DataExchangeManager is not init, while in test '{}'", name),
                Some(data_exchange_manager) => data_exchange_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<DataExchangeManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.data_exchange_manager.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("DataExchangeManager set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<Arc<SessionManager>> for TestGlobalServices {
    fn get(&self) -> Arc<SessionManager> {
        match std::thread::current().name() {
            None => panic!("SessionManager is not init"),
            Some(name) => match self.session_manager.lock().get(name) {
                None => panic!("SessionManager is not init, while in test '{}'", name),
                Some(session_manager) => session_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<SessionManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.session_manager.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("SessionManager set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<Arc<UserApiProvider>> for TestGlobalServices {
    fn get(&self) -> Arc<UserApiProvider> {
        match std::thread::current().name() {
            None => panic!("UserApiProvider is not init"),
            Some(name) => match self.users_manager.lock().get(name) {
                None => panic!("UserApiProvider is not init, while in test '{}'", name),
                Some(users_manager) => users_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<UserApiProvider>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.users_manager.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("UserApiProvider set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<Arc<RoleCacheManager>> for TestGlobalServices {
    fn get(&self) -> Arc<RoleCacheManager> {
        match std::thread::current().name() {
            None => panic!("RoleCacheManager is not init"),
            Some(name) => match self.users_role_manager.lock().get(name) {
                None => panic!("RoleCacheManager is not init, while in test '{}'", name),
                Some(users_role_manager) => users_role_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<RoleCacheManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.users_role_manager.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("RoleCacheManager set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}

impl SingletonImpl<Arc<Config>> for TestGlobalServices {
    fn get(&self) -> Arc<Config> {
        match std::thread::current().name() {
            None => panic!("GlobalConfig is not init"),
            Some(name) => match self.query_config.lock().get(name) {
                None => panic!("GlobalConfig is not init, while in test '{}'", name),
                Some(users_role_manager) => users_role_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<Config>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => match self.query_config.lock().entry(name.to_string()) {
                Entry::Vacant(v) => v.insert(value),
                Entry::Occupied(_v) => panic!("GlobalConfig set twice in test[{:?}]", name),
            },
        };

        Ok(())
    }
}
