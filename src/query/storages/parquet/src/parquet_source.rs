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

use std::any::Any;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::parquet::indexes::Interval;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::parquet_part::ParquetRowGroupPart;
use crate::parquet_reader::IndexedChunk;
use crate::parquet_reader::ParquetReader;
use crate::parquet_source::State::Generated;
use crate::ReadOptions;

struct PrewhereData {
    data_block: DataBlock,
    filter: Value<BooleanType>,
}

/// The states for [`ParquetSource`]. The states will recycle for each row group of a parquet file.
enum State {
    ReadDataPrewhere(Option<PartInfoPtr>),
    ReadDataRemain(PartInfoPtr, PrewhereData, Option<Bitmap>),
    PrewhereFilter(PartInfoPtr, Vec<IndexedChunk>, Option<Bitmap>),
    Deserialize(
        PartInfoPtr,
        Vec<IndexedChunk>,
        Option<PrewhereData>,
        Option<Bitmap>,
    ),
    Generated(Option<PartInfoPtr>, DataBlock),
    Finish,
}

pub struct ParquetSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,
    /// The schema before output. Some fields might be removed when outputing.
    src_schema: DataSchemaRef,
    /// The final output schema
    output_schema: DataSchemaRef,

    prewhere_reader: Arc<ParquetReader>,
    prewhere_filter: Arc<Option<Expr>>,
    remain_reader: Arc<Option<ParquetReader>>,

    read_options: ReadOptions,
}

impl ParquetSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        prewhere_reader: Arc<ParquetReader>,
        prewhere_filter: Arc<Option<Expr>>,
        remain_reader: Arc<Option<ParquetReader>>,
        read_options: ReadOptions,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let mut src_fields = prewhere_reader.output_schema().fields().clone();
        if let Some(reader) = remain_reader.as_ref() {
            let remain_field = reader.output_schema().fields();
            src_fields.extend_from_slice(remain_field);
        }
        let src_schema = DataSchemaRefExt::create(src_fields);

        Ok(ProcessorPtr::create(Box::new(ParquetSource {
            ctx,
            output,
            scan_progress,
            state: State::ReadDataPrewhere(None),
            output_schema,
            src_schema,
            prewhere_reader,
            prewhere_filter,
            remain_reader,
            read_options,
        })))
    }

    fn do_prewhere_filter(
        &mut self,
        part: PartInfoPtr,
        raw_chunks: Vec<IndexedChunk>,
        row_selection: Option<Bitmap>,
    ) -> Result<()> {
        let rg_part = ParquetRowGroupPart::from_part(&part)?;
        // deserialize prewhere data block first
        let data_block = if let Some(row_selection) = &row_selection {
            self.prewhere_reader
                .deserialize(rg_part, raw_chunks, Some(row_selection.clone()))?
        } else {
            self.prewhere_reader
                .deserialize(rg_part, raw_chunks, None)?
        };

        if let Some(filter) = self.prewhere_filter.as_ref() {
            // do filter
            let func_ctx = self.ctx.try_get_function_context()?;
            let evaluator = Evaluator::new(&data_block, func_ctx, &BUILTIN_FUNCTIONS);

            let res = evaluator.run(filter).map_err(|(_, e)| {
                ErrorCode::Internal(format!("eval prewhere filter failed: {}.", e))
            })?;
            let filter = DataBlock::cast_to_nonull_boolean(&res).ok_or_else(|| {
                ErrorCode::BadArguments(
                    "Result of filter expression cannot be converted to boolean.",
                )
            })?;

            let all_filtered = match &filter {
                Value::Scalar(v) => !v,
                Value::Column(bitmap) => bitmap.unset_bits() == bitmap.len(),
            };

            if all_filtered {
                // shortcut:
                // all rows in this block are filtered out
                // turn to begin the next state cycle.
                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);

                // Generate a empty block.
                self.state = Generated(
                    self.ctx.try_get_part(),
                    DataBlock::empty_with_schema(self.output_schema.clone()),
                );
                return Ok(());
            }

            let (rows, bytes) = if self.remain_reader.is_none() {
                (data_block.num_rows(), data_block.memory_size())
            } else {
                (0, 0)
            };

            let filtered_block = match &filter {
                Value::Scalar(_) => data_block,
                Value::Column(bitmap) => DataBlock::filter_with_bitmap(data_block, bitmap)?,
            };

            if self.remain_reader.is_none() {
                // shortcut, we don't need to read remain data
                let progress_values = ProgressValues { rows, bytes };
                self.scan_progress.incr(&progress_values);
                let block =
                    filtered_block.resort(self.src_schema.as_ref(), self.output_schema.as_ref())?;
                self.state = Generated(self.ctx.try_get_part(), block);
            } else {
                self.state = State::ReadDataRemain(
                    part,
                    PrewhereData {
                        data_block: filtered_block,
                        filter,
                    },
                    row_selection,
                );
            }
            Ok(())
        } else {
            Err(ErrorCode::Internal(
                "It's a bug. No need to do prewhere filter",
            ))
        }
    }

    fn do_deserialize(
        &mut self,
        part: PartInfoPtr,
        raw_chunks: Vec<IndexedChunk>,
        prewhere_data: Option<PrewhereData>,
        row_selection: Option<Bitmap>,
    ) -> Result<()> {
        let rg_part = ParquetRowGroupPart::from_part(&part)?;
        let output_block = if let Some(PrewhereData {
            data_block: mut prewhere_block,
            filter,
        }) = prewhere_data
        {
            let block = if raw_chunks.is_empty() {
                prewhere_block
            } else if let Some(remain_reader) = self.remain_reader.as_ref() {
                // If reach in this branch, it means `read_options.do_prewhere = true`
                let remain_block = match filter {
                    Value::Scalar(_) => {
                        // The case of all filtered is already covered in `do_prewhere_filter`.
                        // don't need filter
                        remain_reader.deserialize(rg_part, raw_chunks, None)?
                    }
                    Value::Column(bitmap) => {
                        if !self.read_options.push_down_bitmap() || bitmap.unset_bits() == 0 {
                            let block = if let Some(row_selection) = &row_selection {
                                remain_reader.deserialize(
                                    rg_part,
                                    raw_chunks,
                                    Some(row_selection.clone()),
                                )?
                            } else {
                                remain_reader.deserialize(rg_part, raw_chunks, None)?
                            };
                            DataBlock::filter_with_bitmap(block, &bitmap)?
                        } else {
                            remain_reader.deserialize(rg_part, raw_chunks, Some(bitmap))?
                        }
                    }
                };

                assert_eq!(
                    prewhere_block.num_rows(),
                    remain_block.num_rows(),
                    "prewhere and remain blocks should have same row number. (prewhere: {}, remain: {})",
                    prewhere_block.num_rows(),
                    remain_block.num_rows()
                );

                // Combine two blocks.
                for col in remain_block.columns() {
                    prewhere_block.add_column(col.clone());
                }
                prewhere_block
            } else {
                return Err(ErrorCode::Internal("It's a bug. Need remain reader"));
            };
            block
        } else {
            // There is only prewhere reader.
            assert!(self.remain_reader.is_none());
            self.prewhere_reader
                .deserialize(rg_part, raw_chunks, None)?
        };

        let progress_values = ProgressValues {
            rows: output_block.num_rows(),
            bytes: output_block.memory_size(),
        };
        self.scan_progress.incr(&progress_values);

        let output_block =
            output_block.resort(self.src_schema.as_ref(), self.output_schema.as_ref())?;
        self.state = State::Generated(self.ctx.try_get_part(), output_block);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for ParquetSource {
    fn name(&self) -> String {
        "ParquetSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::ReadDataPrewhere(None)) {
            self.state = match self.ctx.try_get_part() {
                None => State::Finish,
                Some(part) => State::ReadDataPrewhere(Some(part)),
            }
        }

        if matches!(self.state, State::Finish) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Generated(_, _)) {
            if let Generated(part, data_block) = std::mem::replace(&mut self.state, State::Finish) {
                if let Some(part) = part {
                    self.state = State::ReadDataPrewhere(Some(part));
                }
                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::ReadDataPrewhere(_)
            | State::ReadDataRemain(_, _, _)
            | State::PrewhereFilter(_, _, _)
            | State::Deserialize(_, _, _, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadDataPrewhere(Some(part)) => {
                let rg_part = ParquetRowGroupPart::from_part(&part)?;
                let row_selection = rg_part
                    .row_selection
                    .as_ref()
                    .map(|sel| intervals_to_bitmap(sel, rg_part.num_rows));
                let chunks = self.prewhere_reader.sync_read_columns(rg_part)?;
                if self.prewhere_filter.is_some() {
                    self.state = State::PrewhereFilter(part, chunks, row_selection);
                } else {
                    // If there is no prewhere filter, it means there is only the prewhere reader.
                    assert!(self.remain_reader.is_none());
                    // So all the needed columns are read.
                    self.state = State::Deserialize(part, chunks, None, row_selection)
                }
                Ok(())
            }
            State::ReadDataRemain(part, prewhere_data, row_selection) => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let rg_part = ParquetRowGroupPart::from_part(&part)?;
                    let chunks = remain_reader.sync_read_columns(rg_part)?;
                    self.state =
                        State::Deserialize(part, chunks, Some(prewhere_data), row_selection);
                    Ok(())
                } else {
                    Err(ErrorCode::Internal("It's a bug. No remain reader"))
                }
            }
            State::PrewhereFilter(part, chunks, row_selection) => {
                self.do_prewhere_filter(part, chunks, row_selection)
            }
            State::Deserialize(part, chunks, prewhere_data, row_selection) => {
                self.do_deserialize(part, chunks, prewhere_data, row_selection)
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}

/// Convert intervals to a bitmap. The `intervals` represents the row selection across `num_rows`.
fn intervals_to_bitmap(interval: &[Interval], num_rows: usize) -> Bitmap {
    debug_assert!(
        interval.is_empty()
            || interval.last().unwrap().start + interval.last().unwrap().length < num_rows
    );

    let mut bitmap = MutableBitmap::with_capacity(num_rows);
    let mut offset = 0;

    for intv in interval {
        bitmap.extend_constant(intv.start - offset, false);
        bitmap.extend_constant(intv.length, true);
        offset = intv.start + intv.length;
    }
    bitmap.extend_constant(num_rows - offset, false);

    bitmap.into()
}
