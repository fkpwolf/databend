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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use serde_json::Value;

use crate::prelude::*;

#[derive(Clone)]
pub struct BooleanSerializer {
    pub(crate) values: Bitmap,
}

impl BooleanSerializer {
    pub fn try_create(col: &ColumnRef) -> Result<Self> {
        let col: &BooleanColumn = Series::check_get(col)?;
        let values = col.values().clone();
        Ok(Self { values })
    }

    #[inline]
    fn write_field_outer(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        let v = if self.values.get_bit(row_index) {
            &format.true_bytes
        } else {
            &format.false_bytes
        };
        buf.extend_from_slice(v);
    }
}

impl<'a> TypeSerializer<'a> for BooleanSerializer {
    fn write_field_values(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        _in_nested: bool,
    ) {
        let v = if self.values.get_bit(row_index) {
            &format.nested.true_bytes
        } else {
            &format.nested.false_bytes
        };
        buf.extend_from_slice(v);
    }

    fn write_field_tsv(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        _in_nested: bool,
    ) {
        self.write_field_outer(row_index, buf, format)
    }

    fn write_field_csv(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        self.write_field_outer(row_index, buf, format)
    }

    fn write_field_json(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        _quote: bool,
    ) {
        self.write_field_outer(row_index, buf, format)
    }

    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let result: Vec<Value> = self
            .values
            .iter()
            .map(|v| serde_json::to_value(v).unwrap())
            .collect();
        Ok(result)
    }

    fn serialize_json_object(
        &self,
        _valids: Option<&Bitmap>,
        format: &FormatSettings,
    ) -> Result<Vec<Value>> {
        self.serialize_json_values(format)
    }

    fn serialize_json_object_suppress_error(
        &self,
        _format: &FormatSettings,
    ) -> Result<Vec<Option<Value>>> {
        let result: Vec<Option<Value>> = self
            .values
            .iter()
            .map(|x| match serde_json::to_value(x) {
                Ok(v) => Some(v),
                Err(_) => None,
            })
            .collect();
        Ok(result)
    }
}
