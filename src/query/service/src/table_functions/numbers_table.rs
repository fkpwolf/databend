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

use std::any::Any;
use std::mem::size_of;
use std::sync::Arc;

use chrono::NaiveDateTime;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::TableStatistics;
use common_datablocks::DataBlock;
use common_datavalues::chrono::TimeZone;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::EmptySource;
use crate::pipelines::processors::SyncSource;
use crate::pipelines::processors::SyncSourcer;
use crate::pipelines::Pipe;
use crate::pipelines::Pipeline;
use crate::pipelines::SourcePipeBuilder;
use crate::sessions::TableContext;
use crate::storages::Table;
use crate::table_functions::generate_numbers_parts;
use crate::table_functions::numbers_part::NumbersPartInfo;
use crate::table_functions::table_function_factory::TableArgs;
use crate::table_functions::TableFunction;

pub struct NumbersTable {
    table_info: TableInfo,
    total: u64,
}

impl NumbersTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let mut total = None;
        if let Some(args) = &table_args {
            if args.len() == 1 {
                let arg = &args[0];
                total = Some(arg.as_u64()?);
            }
        }

        let total = total.ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "Must have exactly one number argument for table function.{}",
                &table_func_name
            ))
        })?;

        let engine = match table_func_name {
            "numbers" => "SystemNumbers",
            "numbers_mt" => "SystemNumbersMt",
            "numbers_local" => "SystemNumbersLocal",
            _ => unreachable!(),
        };

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: DataSchemaRefExt::create(vec![DataField::new(
                    "number",
                    u64::to_data_type(),
                )]),
                engine: engine.to_string(),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp(0, 0)),
                updated_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp(0, 0)),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(NumbersTable { table_info, total }))
    }
}

#[async_trait::async_trait]
impl Table for NumbersTable {
    fn is_local(&self) -> bool {
        self.name() == "numbers_local"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let max_block_size = ctx.get_settings().get_max_block_size()?;
        let mut limit = None;

        if let Some(extras) = &push_downs {
            if extras.limit.is_some() && extras.filters.is_empty() && extras.order_by.is_empty() {
                // It is allowed to have an error when we can't get sort columns from the expression. For
                // example 'select number from numbers(10) order by number+4 limit 10', the column 'number+4'
                // doesn't exist in the numbers table.
                // For case like that, we ignore the error and don't apply any optimization.

                // No order by case
                limit = extras.limit;
            }
        }
        let total = match limit {
            Some(limit) => std::cmp::min(self.total, limit as u64),
            None => self.total,
        };

        let fake_partitions = (total / max_block_size) + 1;
        let statistics = PartStatistics::new_exact(
            total as usize,
            ((total) * size_of::<u64>() as u64) as usize,
            fake_partitions as usize,
            fake_partitions as usize,
        );

        let mut worker_num = ctx.get_settings().get_max_threads()?;
        if worker_num > fake_partitions {
            worker_num = fake_partitions;
        }

        let parts = generate_numbers_parts(0, worker_num, total);
        Ok((statistics, parts))
    }

    fn table_args(&self) -> Option<Vec<DataValue>> {
        Some(vec![DataValue::UInt64(self.total)])
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        if plan.parts.is_empty() {
            let output = OutputPort::create();
            pipeline.add_pipe(Pipe::SimplePipe {
                inputs_port: vec![],
                outputs_port: vec![output.clone()],
                processors: vec![EmptySource::create(output)?],
            });

            return Ok(());
        }

        let mut source_builder = SourcePipeBuilder::create();

        for part_index in 0..plan.parts.len() {
            let source_ctx = ctx.clone();
            let source_output_port = OutputPort::create();

            source_builder.add_source(
                source_output_port.clone(),
                NumbersSource::create(
                    source_output_port,
                    source_ctx,
                    &plan.parts[part_index],
                    self.schema(),
                )?,
            );
        }

        pipeline.add_pipe(source_builder.finalize());
        Ok(())
    }

    fn table_statistics(&self) -> Result<Option<TableStatistics>> {
        Ok(Some(TableStatistics {
            num_rows: Some(self.total),
            data_size: Some(self.total * 8),
            data_size_compressed: None,
            index_size: None,
        }))
    }
}

struct NumbersSource {
    begin: u64,
    end: u64,
    step: u64,
    schema: DataSchemaRef,
}

impl NumbersSource {
    pub fn create(
        output: Arc<OutputPort>,
        ctx: Arc<dyn TableContext>,
        numbers_part: &PartInfoPtr,
        schema: DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        let settings = ctx.get_settings();
        let numbers_part = NumbersPartInfo::from_part(numbers_part)?;

        SyncSourcer::create(ctx, output, NumbersSource {
            schema,
            begin: numbers_part.part_start,
            end: numbers_part.part_end,
            step: settings.get_max_block_size()?,
        })
    }
}

impl SyncSource for NumbersSource {
    const NAME: &'static str = "NumbersSourceTransform";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        let source_remain_size = self.end - self.begin;

        match source_remain_size {
            0 => Ok(None),
            remain_size => {
                let step = std::cmp::min(remain_size, self.step);
                let column_data = (self.begin..self.begin + step).collect();

                self.begin += step;
                let column = UInt64Column::new_from_vec(column_data);
                Ok(Some(DataBlock::create(self.schema.clone(), vec![
                    Arc::new(column),
                ])))
            }
        }
    }
}

impl TableFunction for NumbersTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
