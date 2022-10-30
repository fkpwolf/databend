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

use std::sync::Arc;

use common_exception::Result;
use common_meta_types::StageType;
use common_planner::plans::DropStagePlan;
use common_users::UserApiProvider;
use tracing::info;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::stage::StageTable;

#[derive(Debug)]
pub struct DropUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropStagePlan,
}

impl DropUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropStagePlan) -> Result<Self> {
        Ok(DropUserStageInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropUserStageInterpreter {
    fn name(&self) -> &str {
        "DropUserStageInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        let stage = user_mgr.get_stage(&tenant, &plan.name).await;
        user_mgr
            .drop_stage(&tenant, &plan.name, plan.if_exists)
            .await?;

        if let Ok(stage) = stage {
            if !matches!(&stage.stage_type, StageType::External) {
                let tctx: Arc<dyn TableContext> = self.ctx.clone();
                let op = StageTable::get_op(&tctx, &stage)?;
                op.batch().remove_all("/").await?;
                info!(
                    "drop stage {:?} with all objects removed in stage",
                    stage.stage_name
                );
            }
        };

        Ok(PipelineBuildResult::create())
    }
}
