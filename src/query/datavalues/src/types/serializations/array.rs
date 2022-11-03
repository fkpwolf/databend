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

use common_exception::Result;
use common_io::prelude::FormatSettings;
use serde_json::Value;

use crate::prelude::*;
use crate::serializations::write_csv_string;
use crate::serializations::write_json_string;

#[derive(Clone)]
pub struct ArraySerializer<'a> {
    offsets: &'a [i64],
    inner: Box<TypeSerializerImpl<'a>>,
}

impl<'a> ArraySerializer<'a> {
    pub fn try_create(column: &'a ColumnRef, inner_type: &DataTypeImpl) -> Result<Self> {
        let column: &ArrayColumn = Series::check_get(column)?;
        let inner = Box::new(inner_type.create_serializer(column.values())?);
        Ok(Self {
            offsets: column.offsets(),
            inner,
        })
    }
}

impl<'a> TypeSerializer<'a> for ArraySerializer<'a> {
    fn write_field_values(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        _in_nested: bool,
    ) {
        let start = self.offsets[row_index] as usize;
        let end = self.offsets[row_index + 1] as usize;
        buf.push(b'[');
        let inner = &self.inner;
        for i in start..end {
            if i != start {
                buf.extend_from_slice(b", ");
            }
            inner.write_field_values(i, buf, format, true);
        }
        buf.push(b']');
    }

    fn write_field_tsv(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        _in_nested: bool,
    ) {
        let start = self.offsets[row_index] as usize;
        let end = self.offsets[row_index + 1] as usize;
        buf.push(b'[');
        let inner = &self.inner;
        for i in start..end {
            if i != start {
                buf.extend_from_slice(b", ");
            }
            inner.write_field_tsv(i, buf, format, true);
        }
        buf.push(b']');
    }

    fn write_field_csv(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        let v = self.to_vec_values(row_index, format);
        write_csv_string(&v, buf, format.quote_char);
    }

    fn write_field_json(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        quote: bool,
    ) {
        let v = self.to_vec_values(row_index, format);
        if quote {
            buf.push(b'\"');
        }
        write_json_string(&v, buf, format);
        if quote {
            buf.push(b'\"');
        }
    }

    fn serialize_json_values(&self, format: &FormatSettings) -> Result<Vec<Value>> {
        let size = self.offsets.len() - 1;
        let mut result = Vec::with_capacity(size);
        let inner = self.inner.serialize_json_values(format)?;
        let mut iter = inner.into_iter();
        for i in 0..size {
            let len = (self.offsets[i + 1] - self.offsets[i]) as usize;
            let chunk = iter.by_ref().take(len).collect();
            result.push(Value::Array(chunk))
        }
        Ok(result)
    }
}
