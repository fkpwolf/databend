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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::mem;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;

use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::Progress;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::ClickhouseFormatType;
use common_formats::FileFormatTypeExt;
use common_io::prelude::FormatSettings;
use common_meta_types::StageFileCompression;
use common_meta_types::StageFileFormatType;
use common_meta_types::UserStageInfo;
use common_settings::Settings;
use opendal::io_util::CompressAlgorithm;
use opendal::Operator;

use crate::processors::sources::input_formats::delimiter::RecordDelimiter;
use crate::processors::sources::input_formats::impls::input_format_csv::InputFormatCSV;
use crate::processors::sources::input_formats::impls::input_format_ndjson::InputFormatNDJson;
use crate::processors::sources::input_formats::impls::input_format_parquet::InputFormatParquet;
use crate::processors::sources::input_formats::impls::input_format_tsv::InputFormatTSV;
use crate::processors::sources::input_formats::input_format_text::InputFormatText;
use crate::processors::sources::input_formats::input_pipeline::StreamingReadBatch;
use crate::processors::sources::input_formats::input_split::SplitInfo;
use crate::processors::sources::input_formats::InputFormat;

const MIN_ROW_PER_BLOCK: usize = 800 * 1000;
const DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD: usize = 100 * 1024 * 1024;

#[derive(Debug)]
pub enum InputPlan {
    CopyInto(Box<CopyIntoPlan>),
    StreamingLoad(StreamPlan),
}

impl InputPlan {
    pub fn as_stream(&self) -> Result<&StreamPlan> {
        match self {
            InputPlan::StreamingLoad(p) => Ok(p),
            _ => Err(ErrorCode::Internal("expect StreamingLoad")),
        }
    }
}

#[derive(Debug)]
pub struct CopyIntoPlan {
    pub stage_info: UserStageInfo,
    pub files: Vec<String>,
}

#[derive(Debug)]
pub struct StreamPlan {
    pub is_multi_part: bool,
    pub compression: StageFileCompression,
}

pub enum InputSource {
    Operator(Operator),
    // need Mutex because Arc<InputContext> is immutable and mpsc receiver can not clone
    Stream(Mutex<Option<Receiver<Result<StreamingReadBatch>>>>),
}

impl InputSource {
    pub fn take_receiver(&self) -> Result<Receiver<Result<StreamingReadBatch>>> {
        match &self {
            InputSource::Operator(_) => Err(ErrorCode::Internal(
                "should not happen: copy with streaming source",
            )),
            InputSource::Stream(i) => {
                let mut guard = i.lock().expect("must success");
                let opt = &mut *guard;
                let r = mem::take(opt).expect("must success");
                Ok(r)
            }
        }
    }

    pub fn get_operator(&self) -> Result<Operator> {
        match self {
            InputSource::Operator(op) => Ok(op.clone()),
            InputSource::Stream(_) => Err(ErrorCode::Internal(
                "should not happen: copy with streaming source",
            )),
        }
    }
}

pub struct InputContext {
    pub plan: InputPlan,
    pub schema: DataSchemaRef,
    pub source: InputSource,
    pub format: Arc<dyn InputFormat>,
    pub splits: Vec<Arc<SplitInfo>>,

    // row format only
    pub rows_to_skip: usize,
    pub field_delimiter: u8,
    pub record_delimiter: RecordDelimiter,

    // runtime config
    pub settings: Arc<Settings>,
    pub format_settings: FormatSettings,

    pub read_batch_size: usize,
    pub rows_per_block: usize,
    pub block_memory_size_threshold: usize,

    pub scan_progress: Arc<Progress>,
}

impl Debug for InputContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputContext")
            .field("plan", &self.plan)
            .field("rows_to_skip", &self.rows_to_skip)
            .field("field_delimiter", &self.field_delimiter)
            .field("record_delimiter", &self.record_delimiter)
            .field("format_settings", &self.format_settings)
            .field("rows_per_block", &self.rows_per_block)
            .field("read_batch_size", &self.read_batch_size)
            .field("num_splits", &self.splits.len())
            .finish()
    }
}

impl InputContext {
    pub fn get_input_format(format: &StageFileFormatType) -> Result<Arc<dyn InputFormat>> {
        match format {
            StageFileFormatType::Tsv => Ok(Arc::new(InputFormatText::<InputFormatTSV>::create())),
            StageFileFormatType::Csv => Ok(Arc::new(InputFormatText::<InputFormatCSV>::create())),
            StageFileFormatType::NdJson => {
                Ok(Arc::new(InputFormatText::<InputFormatNDJson>::create()))
            }
            StageFileFormatType::Parquet => Ok(Arc::new(InputFormatParquet {})),
            format => Err(ErrorCode::Internal(format!(
                "Unsupported file format: {:?}",
                format
            ))),
        }
    }

    pub async fn try_create_from_copy(
        operator: Operator,
        settings: Arc<Settings>,
        schema: DataSchemaRef,
        stage_info: UserStageInfo,
        files: Vec<String>,
        scan_progress: Arc<Progress>,
    ) -> Result<Self> {
        if files.is_empty() {
            return Err(ErrorCode::BadArguments("no file to copy"));
        }
        let plan = Box::new(CopyIntoPlan { stage_info, files });
        let read_batch_size = settings.get_input_read_buffer_size()? as usize;
        let file_format_options = &plan.stage_info.file_format_options;
        let format_typ = file_format_options.format.clone();
        let file_format_options =
            StageFileFormatType::get_ext_from_stage(file_format_options.clone());
        let file_format_options = format_typ.final_file_format_options(&file_format_options)?;

        let format = Self::get_input_format(&format_typ)?;
        let splits = format
            .get_splits(&plan, &operator, &settings, &schema)
            .await?;
        let rows_per_block = MIN_ROW_PER_BLOCK;
        let record_delimiter = {
            if file_format_options.stage.record_delimiter.is_empty() {
                format.default_record_delimiter()
            } else {
                RecordDelimiter::try_from(file_format_options.stage.record_delimiter.as_str())?
            }
        };

        let format_settings = format_typ.get_format_settings(&file_format_options, &settings)?;

        let rows_to_skip = file_format_options.stage.skip_header as usize;
        let field_delimiter = {
            if file_format_options.stage.field_delimiter.is_empty() {
                format.default_field_delimiter()
            } else {
                file_format_options.stage.field_delimiter.as_bytes()[0]
            }
        };

        Ok(InputContext {
            format,
            schema,
            splits,
            settings,
            format_settings,
            record_delimiter,
            rows_per_block,
            read_batch_size,
            rows_to_skip,
            field_delimiter,
            scan_progress,
            source: InputSource::Operator(operator),
            plan: InputPlan::CopyInto(plan),
            block_memory_size_threshold: DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD,
        })
    }

    pub async fn try_create_from_insert(
        format_name: &str,
        stream_receiver: Receiver<Result<StreamingReadBatch>>,
        settings: Arc<Settings>,
        schema: DataSchemaRef,
        scan_progress: Arc<Progress>,
        is_multi_part: bool,
    ) -> Result<Self> {
        let (format_name, rows_to_skip) = remove_clickhouse_format_suffix(format_name);
        let rows_to_skip = std::cmp::max(settings.get_format_skip_header()? as usize, rows_to_skip);

        let file_format_options = if is_multi_part {
            let format_type =
                StageFileFormatType::from_str(format_name).map_err(ErrorCode::UnknownFormat)?;
            format_type.get_file_format_options_from_setting(&settings, None)
        } else {
            // clickhouse
            let typ = ClickhouseFormatType::parse_clickhouse_format(format_name)?;
            typ.typ
                .get_file_format_options_from_setting(&settings, Some(typ.suffixes))
        }?;
        let format_type = file_format_options.stage.format.clone();

        let file_format_options = format_type.final_file_format_options(&file_format_options)?;
        let format = Self::get_input_format(&format_type)?;
        let format_settings = format_type.get_format_settings(&file_format_options, &settings)?;
        let read_batch_size = settings.get_input_read_buffer_size()? as usize;
        let rows_per_block = MIN_ROW_PER_BLOCK;
        let field_delimiter = file_format_options.stage.field_delimiter;
        let field_delimiter = {
            if field_delimiter.is_empty() {
                format.default_field_delimiter()
            } else {
                field_delimiter.as_bytes()[0]
            }
        };
        let record_delimiter =
            RecordDelimiter::try_from(file_format_options.stage.record_delimiter.as_bytes())?;
        let compression = settings.get_format_compression()?;
        let compression = if !compression.is_empty() {
            StageFileCompression::from_str(&compression).map_err(ErrorCode::BadArguments)?
        } else {
            StageFileCompression::Auto
        };
        let plan = StreamPlan {
            is_multi_part,
            compression,
        };

        Ok(InputContext {
            format,
            schema,
            settings,
            format_settings,
            record_delimiter,
            rows_per_block,
            read_batch_size,
            field_delimiter,
            rows_to_skip,
            scan_progress,
            source: InputSource::Stream(Mutex::new(Some(stream_receiver))),
            plan: InputPlan::StreamingLoad(plan),
            splits: vec![],
            block_memory_size_threshold: DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD,
        })
    }

    pub fn num_prefetch_splits(&self) -> Result<usize> {
        Ok(self.settings.get_max_threads()? as usize)
    }

    pub fn num_prefetch_per_split(&self) -> usize {
        1
    }

    pub fn get_compression_alg(&self, path: &str) -> Result<Option<CompressAlgorithm>> {
        let opt = match &self.plan {
            InputPlan::CopyInto(p) => p.stage_info.file_format_options.compression,
            InputPlan::StreamingLoad(p) => p.compression,
        };
        Self::get_compression_alg_copy(opt, path)
    }

    pub fn get_compression_alg_copy(
        compress_option: StageFileCompression,
        path: &str,
    ) -> Result<Option<CompressAlgorithm>> {
        let compression_algo = match compress_option {
            StageFileCompression::Auto => CompressAlgorithm::from_path(path),
            StageFileCompression::Gzip => Some(CompressAlgorithm::Gzip),
            StageFileCompression::Bz2 => Some(CompressAlgorithm::Bz2),
            StageFileCompression::Brotli => Some(CompressAlgorithm::Brotli),
            StageFileCompression::Zstd => Some(CompressAlgorithm::Zstd),
            StageFileCompression::Deflate => Some(CompressAlgorithm::Zlib),
            StageFileCompression::RawDeflate => Some(CompressAlgorithm::Deflate),
            StageFileCompression::Xz => Some(CompressAlgorithm::Xz),
            StageFileCompression::Lzo => {
                return Err(ErrorCode::Unimplemented(
                    "compress type lzo is unimplemented",
                ));
            }
            StageFileCompression::Snappy => {
                return Err(ErrorCode::Unimplemented(
                    "compress type snappy is unimplemented",
                ));
            }
            StageFileCompression::None => None,
        };
        Ok(compression_algo)
    }
}

const WITH_NAMES_AND_TYPES: &str = "withnamesandtypes";
const WITH_NAMES: &str = "withnames";

fn remove_clickhouse_format_suffix(name: &str) -> (&str, usize) {
    let s = name.to_lowercase();
    let (suf_len, skip) = if s.ends_with(WITH_NAMES_AND_TYPES) {
        (WITH_NAMES_AND_TYPES.len(), 2)
    } else if s.ends_with(WITH_NAMES) {
        (WITH_NAMES.len(), 1)
    } else {
        (0, 0)
    };
    (&name[0..(s.len() - suf_len)], skip)
}
