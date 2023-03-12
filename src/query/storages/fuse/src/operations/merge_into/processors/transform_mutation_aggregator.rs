// Copyright 2023 Datafuse Labs.
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
//

use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoPtr;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_pipeline_transforms::processors::transforms::transform_accumulating_async::AsyncAccumulatingTransform;
use opendal::Operator;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CacheManager;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use tracing::debug;

use crate::io::execute_futures_in_parallel;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::merge_into::mutation_meta::mutation_log::CommitMeta;
use crate::operations::merge_into::mutation_meta::mutation_log::MutationLogEntry;
use crate::operations::merge_into::mutation_meta::mutation_log::MutationLogs;
use crate::operations::merge_into::mutation_meta::mutation_log::Replacement;
use crate::operations::merge_into::mutator::mutation_accumulator::MutationAccumulator;
use crate::operations::merge_into::mutator::mutation_accumulator::SerializedSegment;
use crate::operations::mutation::AbortOperation;

// takes in table mutation logs and aggregates them (former mutation_transform)
pub struct TableMutationAggregator {
    mutation_accumulator: MutationAccumulator,
    base_segments: Vec<Location>,
    thresholds: BlockThresholds,
    location_gen: TableMetaLocationGenerator,
    abort_operation: AbortOperation,
    ctx: Arc<dyn TableContext>,
    schema: TableSchemaRef,
    dal: Operator,
}

impl TableMutationAggregator {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        base_segments: Vec<Location>,
        thresholds: BlockThresholds,
        location_gen: TableMetaLocationGenerator,
        schema: TableSchemaRef,
        dal: Operator,
    ) -> Self {
        TableMutationAggregator {
            mutation_accumulator: Default::default(),
            base_segments,
            thresholds,
            location_gen,
            abort_operation: Default::default(),
            ctx,
            schema,
            dal,
        }
    }
}

impl TableMutationAggregator {
    pub fn accumulate_mutation(&mut self, mutations: MutationLogs) {
        for entry in &mutations.entries {
            self.mutation_accumulator.accumulate_log_entry(entry);
            // TODO wrap this aborts in mutation accumulator
            match entry {
                MutationLogEntry::Replacement(mutation) => {
                    if let Replacement::Replaced(block_meta) = &mutation.op {
                        self.abort_operation.add_block(block_meta);
                    }
                }
                MutationLogEntry::Append(append) => {
                    for block_meta in &append.segment_info.blocks {
                        self.abort_operation.add_block(block_meta);
                    }
                    // TODO can we avoid this clone?
                    self.abort_operation
                        .add_segment(append.segment_location.clone());
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for TableMutationAggregator {
    const NAME: &'static str = "MutationAggregator";

    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        let mutation = MutationLogs::try_from(data)?;
        self.accumulate_mutation(mutation);
        Ok(None)
    }

    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        let mutations: CommitMeta = self.apply_mutations().await?;
        debug!("mutations {:?}", mutations);
        let block_meta: BlockMetaInfoPtr = Box::new(mutations);
        Ok(Some(DataBlock::empty_with_meta(block_meta)))
    }
}

impl TableMutationAggregator {
    async fn apply_mutations(&mut self) -> Result<CommitMeta> {
        let base_segments_paths = self.base_segments.clone();
        // NOTE: order matters!
        let segment_infos = self.read_segments().await?;

        let (commit_meta, serialized_segments) = self.mutation_accumulator.apply(
            base_segments_paths,
            &segment_infos,
            self.thresholds,
            &self.location_gen,
        )?;

        self.write_segments(serialized_segments).await?;
        Ok::<_, ErrorCode>(commit_meta)
    }

    async fn read_segments(&self) -> Result<Vec<Arc<SegmentInfo>>> {
        let segments_io =
            SegmentsIO::create(self.ctx.clone(), self.dal.clone(), self.schema.clone());
        let segment_locations = self.base_segments.as_slice();
        let segments = segments_io
            .read_segments(segment_locations)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        Ok(segments)
    }

    // TODO use batch_meta_writer
    async fn write_segments(&self, segments: Vec<SerializedSegment>) -> Result<()> {
        let mut handles = Vec::with_capacity(segments.len());
        for segment in segments {
            let op = self.dal.clone();
            handles.push(async move {
                op.write(&segment.path, segment.raw_data).await?;
                if let Some(segment_cache) = CacheManager::instance().get_table_segment_cache() {
                    segment_cache.put(segment.path.clone(), segment.segment.clone());
                }
                Ok::<_, ErrorCode>(())
            });
        }

        execute_futures_in_parallel(
            self.ctx.clone(),
            handles,
            "mutation-write-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}
