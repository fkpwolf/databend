// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common_base::base::GlobalInstance;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_storages_fuse::FuseTable;
use vacuum_handler::VacuumHandler;
use vacuum_handler::VacuumHandlerWrapper;

use crate::storages::fuse::do_vacuum;

pub struct RealVacuumHandler {}

#[async_trait::async_trait]
impl VacuumHandler for RealVacuumHandler {
    async fn do_vacuum(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        retention_time: DateTime<Utc>,
        dry_run_limit: Option<usize>,
    ) -> Result<Option<Vec<String>>> {
        do_vacuum(fuse_table, ctx, retention_time, dry_run_limit).await
    }
}

impl RealVacuumHandler {
    pub fn init() -> Result<()> {
        let rm = RealVacuumHandler {};
        let wrapper = VacuumHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
