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

use std::sync::Arc;

use common_base::base::Singleton;
use common_exception::Result;
use once_cell::sync::OnceCell;

use crate::Config;

pub struct GlobalConfig;

static GLOBAL_CONFIG: OnceCell<Singleton<Arc<Config>>> = OnceCell::new();

impl GlobalConfig {
    pub fn init(config: Config, v: Singleton<Arc<Config>>) -> Result<()> {
        v.init(Arc::new(config))?;
        GLOBAL_CONFIG.set(v).ok();
        Ok(())
    }

    pub fn instance() -> Arc<Config> {
        match GLOBAL_CONFIG.get() {
            None => panic!("GlobalConfig is not init"),
            Some(config) => config.get(),
        }
    }
}
