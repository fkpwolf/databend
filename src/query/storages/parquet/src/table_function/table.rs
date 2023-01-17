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
use std::fs::File;
use std::sync::Arc;

use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read as pread;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_function::TableFunction;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Scalar;
use common_expression::TableSchema;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::Pipeline;
use opendal::Operator;

use super::TableContext;
use crate::parquet_part::ParquetLocationPart;
use crate::ReadOptions;

pub struct ParquetTable {
    table_args: Vec<Scalar>,

    file_locations: Vec<String>,
    pub(super) table_info: TableInfo,
    pub(super) arrow_schema: ArrowSchema,
    pub(super) operator: Operator,
    pub(super) read_options: ReadOptions,
}

impl ParquetTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        if !GlobalConfig::instance().storage.allow_insecure {
            return Err(ErrorCode::StorageInsecure(
                "Should enable `allow_insecure` to use table function `read_parquet`",
            ));
        }

        if table_args.is_none() || table_args.as_ref().unwrap().is_empty() {
            return Err(ErrorCode::BadArguments(
                "read_parquet needs at least one argument",
            ));
        }

        let table_args = table_args.unwrap();

        let mut file_locations = Vec::with_capacity(table_args.len());
        for arg in table_args.iter() {
            match arg {
                Scalar::String(path) => {
                    let maybe_glob_path = std::str::from_utf8(path).unwrap();
                    let paths = glob::glob(maybe_glob_path)
                        .map_err(|e| ErrorCode::Internal(format!("glob error: {}", e)))?;
                    for entry in paths {
                        match entry {
                            Ok(path) => {
                                file_locations.push(path.to_string_lossy().to_string());
                            }
                            Err(e) => {
                                return Err(ErrorCode::Internal(format!("glob error: {}", e)));
                            }
                        }
                    }
                }
                _ => {
                    return Err(ErrorCode::BadArguments(
                        "read_parquet only accepts string arguments",
                    ));
                }
            }
        }

        if file_locations.is_empty() {
            return Err(ErrorCode::BadArguments(
                "No matched files found for read_parquet",
            ));
        }

        // Infer schema from the first parquet file.
        // Assume all parquet files have the same schema.
        // If not, throw error during reading.
        let location = &file_locations[0];
        let mut file = File::open(location).map_err(|e| {
            ErrorCode::Internal(format!("Failed to open file '{}': {}", location, e))
        })?;
        let first_meta = pread::read_metadata(&mut file).map_err(|e| {
            ErrorCode::Internal(format!(
                "Read parquet file '{}''s meta error: {}",
                location, e
            ))
        })?;
        let arrow_schema = pread::infer_schema(&first_meta)?;

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: arrow_to_table_schema(arrow_schema.clone()).into(),
                engine: "SystemReadParquet".to_string(),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: Utc
                    .from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
                updated_on: Utc
                    .from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
                ..Default::default()
            },
            ..Default::default()
        };

        let mut builder = opendal::services::fs::Builder::default();
        builder.root("/");
        let operator = Operator::new(builder.build()?);

        Ok(Arc::new(ParquetTable {
            table_args,
            file_locations,
            table_info,
            arrow_schema,
            operator,
            read_options: ReadOptions::new()
                .with_prune_row_groups()
                .with_prune_pages()
                .with_do_prewhere(), // Now, `read_options` is hard-coded.
        }))
    }
}

#[async_trait::async_trait]
impl Table for ParquetTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn benefit_column_prune(&self) -> bool {
        true
    }

    fn support_prewhere(&self) -> bool {
        self.read_options.do_prewhere()
    }

    fn has_exact_total_row_count(&self) -> bool {
        true
    }

    fn table_args(&self) -> Option<Vec<Scalar>> {
        Some(self.table_args.clone())
    }

    /// The returned partitions only record the locations of files to read.
    /// So they don't have any real statistics.
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((
            PartStatistics::new_estimated(
                0,
                0,
                self.file_locations.len(),
                self.file_locations.len(),
            ),
            Partitions::create(
                PartitionsShuffleKind::Mod,
                self.file_locations
                    .iter()
                    .map(|location| ParquetLocationPart::create(location.clone()))
                    .collect(),
            ),
        ))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline)
    }
}

impl TableFunction for ParquetTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

fn lower_field_name(field: &mut ArrowField) {
    field.name = field.name.to_lowercase();
    match &mut field.data_type {
        ArrowDataType::List(f)
        | ArrowDataType::LargeList(f)
        | ArrowDataType::FixedSizeList(f, _) => {
            lower_field_name(f.as_mut());
        }
        ArrowDataType::Struct(ref mut fields) => {
            for f in fields {
                lower_field_name(f);
            }
        }
        _ => {}
    }
}

pub fn arrow_to_table_schema(mut schema: ArrowSchema) -> TableSchema {
    schema.fields.iter_mut().for_each(|f| {
        lower_field_name(f);
    });
    TableSchema::from(&schema)
}
