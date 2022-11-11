//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_exception::Result;
use tracing::info;

use crate::interpreters::fragments::Fragmenter;
use crate::interpreters::fragments::QueryFragmentsActions;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalPlan;
use crate::sql::ColumnBinding;

#[tracing::instrument(level = "debug", skip_all)]
pub async fn schedule_query_v2(
    ctx: Arc<QueryContext>,
    result_columns: &[ColumnBinding],
    plan: &PhysicalPlan,
) -> Result<PipelineBuildResult> {
    if !plan.is_distributed_plan() {
        let pb = PipelineBuilder::create(ctx.clone());
        let mut build_res = pb.finalize(plan)?;
        PipelineBuilder::render_result_set(
            &ctx.try_get_function_context()?,
            plan.output_schema()?,
            result_columns,
            &mut build_res.main_pipeline,
        )?;
        build_res.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
        return Ok(build_res);
    }

    let mut build_res = build_schedule_pipeline(ctx.clone(), plan).await?;

    let input_schema = plan.output_schema()?;
    PipelineBuilder::render_result_set(
        &ctx.try_get_function_context()?,
        input_schema,
        result_columns,
        &mut build_res.main_pipeline,
    )?;
    Ok(build_res)
}

pub async fn build_schedule_pipeline(
    ctx: Arc<QueryContext>,
    plan: &PhysicalPlan,
) -> Result<PipelineBuildResult> {
    let fragmenter = Fragmenter::try_create(ctx.clone())?;
    info!("schedule_query_v2 PhysicalPlan:\n{}", plan.format_indent(1));
    let root_fragment = fragmenter.build_fragment(plan)?;

    let mut fragments_actions = QueryFragmentsActions::create(ctx.clone());
    root_fragment.get_actions(ctx.clone(), &mut fragments_actions)?;

    let exchange_manager = ctx.get_exchange_manager();

    info!(
        "schedule_query_v2 QueryFragmentActions:\n{}",
        fragments_actions.display_indent()
    );
    info!(
        "schedule_query_v2 QueryFragmentActions:\n{:?}", /* 2 fragments. first one has been splittd by node.  all secret, but not pretty print */
        fragments_actions
    );

    // invoke to send 3 messages
    let mut build_res = exchange_manager
        .commit_actions(ctx.clone(), fragments_actions)
        .await?;

    let settings = ctx.get_settings();
    build_res.set_max_threads(settings.get_max_threads()? as usize);
    Ok(build_res)
}
