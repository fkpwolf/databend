//  Copyright 2022 Datafuse Labs.
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

use common_base::base::ProgressValues;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PruningStatistics;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::filter_helper::FilterHelpers;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::RemoteExpr;
use common_expression::TableSchema;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_sql::evaluator::BlockOperator;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::TableSnapshot;

use crate::operations::mutation::MutationAction;
use crate::operations::mutation::MutationPartInfo;
use crate::operations::mutation::MutationSink;
use crate::operations::mutation::MutationSource;
use crate::operations::mutation::MutationTransform;
use crate::operations::mutation::SerializeDataTransform;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::Pipeline;
use crate::pruning::FusePruner;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

impl FuseTable {
    /// The flow of Pipeline is as follows:
    /// +---------------+      +-----------------------+
    /// |MutationSource1| ---> |SerializeDataTransform1|   ------
    /// +---------------+      +-----------------------+         |      +-----------------+      +------------+
    /// |     ...       | ---> |          ...          |   ...   | ---> |MutationTransform| ---> |MutationSink|
    /// +---------------+      +-----------------------+         |      +-----------------+      +------------+
    /// |MutationSourceN| ---> |SerializeDataTransformN|   ------
    /// +---------------+      +-----------------------+
    pub async fn do_delete(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
        col_indices: Vec<usize>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let snapshot_opt = self.read_table_snapshot().await?;

        // check if table is empty
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no deletion
            return Ok(());
        };

        if snapshot.summary.row_count == 0 {
            // empty snapshot, no deletion
            return Ok(());
        }

        let scan_progress = ctx.get_scan_progress();
        // check if unconditional deletion
        if filter.is_none() {
            let progress_values = ProgressValues {
                rows: snapshot.summary.row_count as usize,
                bytes: snapshot.summary.uncompressed_byte_size as usize,
            };
            scan_progress.incr(&progress_values);
            // deleting the whole table... just a truncate
            let purge = false;
            return self.do_truncate(ctx.clone(), purge).await;
        }

        let filter_expr = filter.unwrap();
        if col_indices.is_empty() {
            // here the situation: filter_expr is not null, but col_indices in empty, which
            // indicates the expr being evaluated is unrelated to the value of rows:
            //   e.g.
            //       `delete from t where 1 = 1`, `delete from t where now()`,
            //       or `delete from t where RANDOM()::INT::BOOLEAN`
            // if the `filter_expr` is of "constant" nullary :
            //   for the whole block, whether all of the rows should be kept or dropped,
            //   we can just return from here, without accessing the block data
            if self.try_eval_const(ctx.clone(), &self.schema(), &filter_expr)? {
                let progress_values = ProgressValues {
                    rows: snapshot.summary.row_count as usize,
                    bytes: snapshot.summary.uncompressed_byte_size as usize,
                };
                scan_progress.incr(&progress_values);

                // deleting the whole table... just a truncate
                let purge = false;
                return self.do_truncate(ctx.clone(), purge).await;
            }
            // do nothing.
            return Ok(());
        }

        self.try_add_deletion_source(ctx.clone(), &filter_expr, col_indices, &snapshot, pipeline)
            .await?;

        let cluster_stats_gen = self.cluster_stats_gen(ctx.clone())?;
        pipeline.add_transform(|input, output| {
            SerializeDataTransform::try_create(
                ctx.clone(),
                input,
                output,
                self,
                cluster_stats_gen.clone(),
            )
        })?;

        self.try_add_mutation_transform(ctx.clone(), snapshot.segments.clone(), pipeline)?;

        pipeline.add_sink(|input| {
            MutationSink::try_create(self, ctx.clone(), snapshot.clone(), input)
        })?;
        Ok(())
    }

    pub fn try_eval_const(
        &self,
        ctx: Arc<dyn TableContext>,
        schema: &TableSchema,
        filter: &RemoteExpr<String>,
    ) -> Result<bool> {
        let dummy_field = DataField::new("dummy", DataType::Null);
        let _dummy_schema = Arc::new(DataSchema::new(vec![dummy_field]));
        let dummy_value = Value::Column(Column::Null { len: 1 });
        let dummy_block = DataBlock::new(
            vec![BlockEntry {
                data_type: DataType::Null,
                value: dummy_value,
            }],
            1,
        );

        let filter_expr = filter
            .as_expr(&BUILTIN_FUNCTIONS)
            .project_column_ref(|name| schema.index_of(name).unwrap());
        let func_ctx = ctx.get_function_context()?;
        let evaluator = Evaluator::new(&dummy_block, func_ctx, &BUILTIN_FUNCTIONS);
        let res = evaluator
            .run(&filter_expr)
            .map_err(|e| e.add_message("eval try eval const failed:"))?;
        let predicates = FilterHelpers::cast_to_nonull_boolean(&res).ok_or_else(|| {
            ErrorCode::BadArguments("Result of filter expression cannot be converted to boolean.")
        })?;

        Ok(match &predicates {
            Value::Scalar(v) => *v,
            Value::Column(bitmap) => bitmap.unset_bits() == 0,
        })
    }

    async fn try_add_deletion_source(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: &RemoteExpr<String>,
        col_indices: Vec<usize>,
        base_snapshot: &TableSnapshot,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let projection = Projection::Columns(col_indices.clone());
        self.mutation_block_purning(
            ctx.clone(),
            vec![filter.clone()],
            projection.clone(),
            base_snapshot,
        )
        .await?;

        let block_reader = self.create_block_reader(projection)?;
        let schema = block_reader.schema();
        let filter = Arc::new(Some(
            filter
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| schema.index_of(name).unwrap()),
        ));

        let all_col_ids = self.all_the_columns_ids();
        let remain_col_ids: Vec<usize> = all_col_ids
            .into_iter()
            .filter(|id| !col_indices.contains(id))
            .collect();
        let mut source_col_ids = col_indices;
        let remain_reader = if remain_col_ids.is_empty() {
            Arc::new(None)
        } else {
            source_col_ids.extend_from_slice(&remain_col_ids);
            Arc::new(Some(
                (*self.create_block_reader(Projection::Columns(remain_col_ids))?).clone(),
            ))
        };

        // resort the block.
        let mut projection = (0..source_col_ids.len()).collect::<Vec<_>>();
        projection.sort_by_key(|&i| source_col_ids[i]);
        let ops = vec![BlockOperator::Project { projection }];

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        // Add source pipe.
        pipeline.add_source(
            |output| {
                MutationSource::try_create(
                    ctx.clone(),
                    MutationAction::Deletion,
                    output,
                    filter.clone(),
                    block_reader.clone(),
                    remain_reader.clone(),
                    ops.clone(),
                )
            },
            max_threads,
        )
    }

    pub async fn mutation_block_purning(
        &self,
        ctx: Arc<dyn TableContext>,
        filters: Vec<RemoteExpr<String>>,
        projection: Projection,
        base_snapshot: &TableSnapshot,
    ) -> Result<()> {
        let push_down = Some(PushDownInfo {
            projection: Some(projection),
            filters,
            ..PushDownInfo::default()
        });

        let segment_locations = base_snapshot.segments.clone();
        let pruner = FusePruner::create(
            &ctx,
            self.operator.clone(),
            self.table_info.schema(),
            &push_down,
        )?;
        let block_metas = pruner.pruning(segment_locations).await?;

        let range_block_metas = block_metas
            .clone()
            .into_iter()
            .map(|(a, b)| (a.range, b))
            .collect::<Vec<_>>();

        let (_, inner_parts) = self.read_partitions_with_metas(
            self.table_info.schema(),
            None,
            &range_block_metas,
            base_snapshot.summary.block_count as usize,
            PruningStatistics::default(),
        )?;

        let parts = Partitions::create(
            PartitionsShuffleKind::Mod,
            block_metas
                .into_iter()
                .zip(inner_parts.partitions.into_iter())
                .map(|(a, c)| MutationPartInfo::create(a.0, a.1.cluster_stats.clone(), c))
                .collect(),
        );
        ctx.set_partitions(parts)
    }

    pub fn try_add_mutation_transform(
        &self,
        ctx: Arc<dyn TableContext>,
        base_segments: Vec<Location>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        if pipeline.is_empty() {
            return Err(ErrorCode::Internal("The pipeline is empty."));
        }

        match pipeline.output_len() {
            0 => Err(ErrorCode::Internal("The output of the last pipe is 0.")),
            last_pipe_size => {
                let mut inputs_port = Vec::with_capacity(last_pipe_size);
                for _ in 0..last_pipe_size {
                    inputs_port.push(InputPort::create());
                }
                let output_port = OutputPort::create();
                pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
                    MutationTransform::try_create(
                        ctx,
                        self.schema(),
                        inputs_port.clone(),
                        output_port.clone(),
                        self.get_operator(),
                        self.meta_location_generator().clone(),
                        base_segments,
                        self.get_block_compact_thresholds(),
                    )?,
                    inputs_port,
                    vec![output_port],
                )]));

                Ok(())
            }
        }
    }

    pub fn cluster_stats_gen(&self, ctx: Arc<dyn TableContext>) -> Result<ClusterStatsGenerator> {
        if self.cluster_key_meta.is_none() {
            return Ok(ClusterStatsGenerator::default());
        }

        let input_schema = self.table_info.schema();
        let mut merged: Vec<DataField> =
            input_schema.fields().iter().map(DataField::from).collect();
        let func_ctx = ctx.get_function_context()?;
        let cluster_keys = self.cluster_keys(ctx);
        let mut cluster_key_index = Vec::with_capacity(cluster_keys.len());
        let mut extra_key_num = 0;
        let mut operators = Vec::with_capacity(cluster_keys.len());

        for remote_expr in &cluster_keys {
            let expr: Expr = remote_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| input_schema.index_of(name).unwrap());
            let index = match &expr {
                Expr::ColumnRef { id, .. } => *id,
                _ => {
                    let cname = format!("{}", expr);
                    merged.push(DataField::new(cname.as_str(), expr.data_type().clone()));
                    operators.push(BlockOperator::Map { expr });

                    let offset = merged.len() - 1;
                    extra_key_num += 1;
                    offset
                }
            };
            cluster_key_index.push(index);
        }

        let max_page_size = if self.is_native() {
            Some(self.get_write_settings().max_page_size)
        } else {
            None
        };

        Ok(ClusterStatsGenerator::new(
            self.cluster_key_meta.as_ref().unwrap().0,
            cluster_key_index,
            extra_key_num,
            max_page_size,
            0,
            self.get_block_compact_thresholds(),
            operators,
            merged,
            func_ctx,
        ))
    }

    pub fn all_the_columns_ids(&self) -> Vec<usize> {
        (0..self.table_info.schema().fields().len())
            .into_iter()
            .collect::<Vec<usize>>()
    }
}
