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

use std::any::Any;
use std::sync::Arc;

use common_base::base::tokio::time::sleep;
use common_base::base::tokio::time::Duration;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_planner::PartInfoPtr;
use opendal::Operator;

use crate::hive_parquet_block_reader::DataBlockDeserializer;
use crate::hive_parquet_block_reader::HiveParquetBlockReader;
use crate::HiveBlockFilter;
use crate::HiveBlocks;
use crate::HivePartInfo;

enum State {
    /// Read parquet file meta data
    /// IO bound
    ReadMeta(Option<PartInfoPtr>),

    /// Read blocks from data groups (without deserialization)
    /// IO bound
    ReadData(HiveBlocks),

    /// Deserialize block from the given data groups
    /// CPU bound
    Deserialize(HiveBlocks, DataBlockDeserializer),

    /// `(_, _, Some(_))` indicates that a data block is ready, and needs to be consumed
    /// `(_, _, None)` indicates that there are no more blocks left for the current row group of `HiveBlocks`
    Generated(HiveBlocks, DataBlockDeserializer, Option<DataBlock>),
    Finish,
}

pub struct HiveTableSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    dal: Operator,
    scan_progress: Arc<Progress>,
    block_reader: Arc<HiveParquetBlockReader>,
    output: Arc<OutputPort>,
    delay: usize,
    hive_block_filter: Arc<HiveBlockFilter>,
}

impl HiveTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        output: Arc<OutputPort>,
        block_reader: Arc<HiveParquetBlockReader>,
        delay: usize,
        hive_block_filter: Arc<HiveBlockFilter>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(HiveTableSource {
            ctx,
            dal,
            output,
            block_reader,
            hive_block_filter,
            scan_progress,
            state: State::ReadMeta(None),
            delay,
        })))
    }

    fn try_get_partitions(&mut self) -> Result<()> {
        match self.ctx.try_get_part() {
            None => self.state = State::Finish,
            Some(part_info) => {
                self.state = State::ReadMeta(Some(part_info));
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for HiveTableSource {
    fn name(&self) -> String {
        "HiveEngineSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::ReadMeta(None)) {
            match self.ctx.try_get_part() {
                None => self.state = State::Finish,
                Some(part_info) => {
                    self.state = State::ReadMeta(Some(part_info));
                }
            }
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Finish) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if matches!(self.state, State::Generated(_, _, _)) {
            if let State::Generated(mut hive_blocks, rowgroup_deserializer, data_block) =
                std::mem::replace(&mut self.state, State::Finish)
            {
                if let Some(data_block) = data_block {
                    let progress_values = ProgressValues {
                        rows: data_block.num_rows(),
                        bytes: data_block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);
                    self.output.push_data(Ok(data_block));
                    self.state = State::Deserialize(hive_blocks, rowgroup_deserializer);
                    return Ok(Event::NeedConsume);
                }

                // read current rowgroup finished, try read next rowgroup
                hive_blocks.advance();
                match hive_blocks.has_blocks() {
                    true => {
                        self.state = State::ReadData(hive_blocks);
                    }
                    false => {
                        self.try_get_partitions()?;
                    }
                }
            }
        }

        match self.state {
            State::Finish => {
                self.output.finish();
                Ok(Event::Finished)
            }
            State::ReadMeta(_) => Ok(Event::Async),
            State::ReadData(_) => Ok(Event::Async),
            State::Deserialize(_, _) => Ok(Event::Sync),
            State::Generated(_, _, _) => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Deserialize(hive_blocks, mut rowgroup_deserializer) => {
                let data_block = self
                    .block_reader
                    .create_data_block(&mut rowgroup_deserializer, hive_blocks.part.clone())?;

                self.state = State::Generated(hive_blocks, rowgroup_deserializer, data_block);
                Ok(())
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadMeta(Some(part)) => {
                if self.delay > 0 {
                    sleep(Duration::from_millis(self.delay as u64)).await;
                    tracing::debug!("sleep for {}ms", self.delay);
                    self.delay = 0;
                }
                let part = HivePartInfo::from_part(&part)?;
                let file_meta = self
                    .block_reader
                    .read_meta_data(self.dal.clone(), &part.filename, part.filesize)
                    .await?;
                let mut hive_blocks =
                    HiveBlocks::create(file_meta, part.clone(), self.hive_block_filter.clone());

                match hive_blocks.prune() {
                    true => {
                        self.state = State::ReadData(hive_blocks);
                    }
                    false => {
                        self.try_get_partitions()?;
                    }
                }
                Ok(())
            }
            State::ReadData(hive_blocks) => {
                let row_group = hive_blocks.get_current_row_group_meta_data();
                let part = hive_blocks.get_part_info();
                let chunks = self
                    .block_reader
                    .read_columns_data(row_group, &part)
                    .await?;
                let rowgroup_deserializer = self
                    .block_reader
                    .create_rowgroup_deserializer(chunks, row_group)?;
                self.state = State::Deserialize(hive_blocks, rowgroup_deserializer);
                Ok(())
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}
