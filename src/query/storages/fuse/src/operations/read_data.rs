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

use common_base::runtime::Runtime;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::Pipeline;

use crate::fuse_lazy_part::FuseLazyPartInfo;
use crate::io::BlockReader;
use crate::operations::fuse_source::build_fuse_source_pipeline;
use crate::FuseTable;

impl FuseTable {
    pub fn create_block_reader(&self, projection: Projection) -> Result<Arc<BlockReader>> {
        let table_schema = self.table_info.schema();
        BlockReader::create(self.operator.clone(), table_schema, projection)
    }

    // Build the block reader.
    fn build_block_reader(&self, plan: &DataSourcePlan) -> Result<Arc<BlockReader>> {
        self.create_block_reader(PushDownInfo::projection_of_push_downs(
            &self.table_info.schema(),
            &plan.push_downs,
        ))
    }

    fn adjust_io_request(&self, ctx: &Arc<dyn TableContext>) -> Result<usize> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;
        Ok(std::cmp::max(max_threads, max_io_requests))
    }

    #[inline]
    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let mut lazy_init_segments = Vec::with_capacity(plan.parts.len());

        for part in &plan.parts.partitions {
            if let Some(lazy_part_info) = part.as_any().downcast_ref::<FuseLazyPartInfo>() {
                lazy_init_segments.push(lazy_part_info.segment_location.clone());
            }
        }

        if !lazy_init_segments.is_empty() {
            let table = self.clone();
            let table_info = self.table_info.clone();
            let push_downs = plan.push_downs.clone();
            let query_ctx = ctx.clone();
            let dal = self.operator.clone();

            // TODO: need refactor
            pipeline.set_on_init(move || {
                let table = table.clone();
                let table_info = table_info.clone();
                let ctx = query_ctx.clone();
                let dal = dal.clone();
                let push_downs = push_downs.clone();
                let lazy_init_segments = lazy_init_segments.clone();

                let partitions = Runtime::with_worker_threads(2, None)?.block_on(async move {
                    let (_statistics, partitions) = table
                        .prune_snapshot_blocks(
                            ctx,
                            dal,
                            push_downs,
                            table_info,
                            lazy_init_segments,
                            0,
                        )
                        .await?;

                    Result::<_, ErrorCode>::Ok(partitions)
                })?;

                query_ctx.try_set_partitions(partitions)?;

                Ok(())
            });
        }

        let block_reader = self.build_block_reader(plan)?;
        let max_io_requests = self.adjust_io_request(&ctx)?;

        build_fuse_source_pipeline(
            ctx,
            pipeline,
            self.storage_format,
            block_reader,
            plan,
            max_io_requests,
        )
    }
}
