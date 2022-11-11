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

use bstr::ByteSlice;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::TypeSerializer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::field_encoder::FieldEncoderRowBased;
use common_formats::field_encoder::FieldEncoderValues;
use common_io::prelude::FormatSettings;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone)]
pub struct JsonBlock {
    pub(crate) data: Vec<Vec<JsonValue>>,
    pub(crate) schema: DataSchemaRef,
}

pub type JsonBlockRef = Arc<JsonBlock>;

pub fn block_to_json_value_columns(
    block: &DataBlock,
    format: &FormatSettings,
) -> Result<Vec<Vec<JsonValue>>> {
    if block.is_empty() {
        return Ok(vec![]);
    }
    let mut col_table = Vec::new();
    let columns_size = block.columns().len();
    for col_index in 0..columns_size {
        let column = block.column(col_index);
        let column = column.convert_full_column();
        let field = block.schema().field(col_index);
        let data_type = field.data_type();
        let serializer = data_type.create_serializer(&column)?;
        col_table.push(serializer.serialize_json_values(format).map_err(|e| {
            ErrorCode::Internal(format!(
                "fail to serialize filed {}, error = {}",
                field.name(),
                e
            ))
        })?);
    }
    Ok(col_table)
}

pub fn block_to_json_value(
    block: &DataBlock,
    format: &FormatSettings,
    string_fields: bool,
) -> Result<Vec<Vec<JsonValue>>> {
    if string_fields {
        block_to_json_value_string_fields(block, format)
    } else {
        block_to_json_value_ast(block, format)
    }
}

fn block_to_json_value_ast(
    block: &DataBlock,
    format: &FormatSettings,
) -> Result<Vec<Vec<JsonValue>>> {
    let cols = block_to_json_value_columns(block, format)?;
    Ok(transpose(cols))
}

fn block_to_json_value_string_fields(
    block: &DataBlock,
    format: &FormatSettings,
) -> Result<Vec<Vec<JsonValue>>> {
    if block.is_empty() {
        return Ok(vec![]);
    }
    let rows_size = block.column(0).len();
    let mut res = Vec::new();
    let serializers = block.get_serializers()?;
    let encoder = FieldEncoderValues::create_for_handler(format.timezone);
    let mut buf = vec![];
    for row_index in 0..rows_size {
        let mut row: Vec<JsonValue> = Vec::with_capacity(block.num_columns());
        for serializer in serializers.iter() {
            buf.clear();
            encoder.write_field(serializer, row_index, &mut buf, true);
            row.push(serde_json::to_value(
                buf.to_str()
                    .map_err(|e| ErrorCode::BadBytes(format!("{}", e)))?,
            )?);
        }
        res.push(row)
    }
    Ok(res)
}

impl JsonBlock {
    pub fn empty() -> Self {
        Self {
            data: vec![],
            schema: Arc::new(DataSchema::empty()),
        }
    }

    pub fn new(block: &DataBlock, format: &FormatSettings, string_fields: bool) -> Result<Self> {
        Ok(JsonBlock {
            data: block_to_json_value(block, format, string_fields)?,
            schema: block.schema().clone(),
        })
    }

    pub fn concat(blocks: Vec<JsonBlock>) -> Self {
        if blocks.is_empty() {
            return Self::empty();
        }
        let schema = blocks[0].schema.clone();
        let results = blocks.into_iter().map(|b| b.data).collect::<Vec<_>>();
        let data = results.concat();
        Self { data, schema }
    }

    pub fn num_rows(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn data(&self) -> &Vec<Vec<JsonValue>> {
        &self.data
    }

    pub fn schema(&self) -> &DataSchemaRef {
        &self.schema
    }
}

impl From<JsonBlock> for Vec<Vec<JsonValue>> {
    fn from(block: JsonBlock) -> Self {
        block.data
    }
}

fn transpose(col_table: Vec<Vec<JsonValue>>) -> Vec<Vec<JsonValue>> {
    if col_table.is_empty() {
        return vec![];
    }
    let num_row = col_table[0].len();
    let mut row_table = Vec::with_capacity(num_row);
    for _ in 0..num_row {
        row_table.push(Vec::with_capacity(col_table.len()));
    }
    for col in col_table {
        for (row_index, row) in row_table.iter_mut().enumerate() {
            row.push(col[row_index].clone());
        }
    }
    row_table
}
