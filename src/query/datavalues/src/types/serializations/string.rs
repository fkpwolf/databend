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
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use serde_json::Value;

pub use super::helper::json::write_json_string;
use crate::prelude::*;

#[derive(Clone)]
pub struct StringSerializer<'a> {
    pub column: &'a StringColumn,
}

impl<'a> StringSerializer<'a> {
    pub fn try_create(col: &'a ColumnRef) -> Result<Self> {
        let column: &StringColumn = Series::check_get(col)?;
        Ok(Self { column })
    }
}

impl<'a> TypeSerializer<'a> for StringSerializer<'a> {
    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let result: Vec<Value> = self
            .column
            .iter()
            .map(|x| serde_json::to_value(String::from_utf8_lossy(x).to_string()).unwrap())
            .collect();
        Ok(result)
    }

    fn serialize_json_object(
        &self,
        valids: Option<&Bitmap>,
        _format: &FormatSettings,
    ) -> Result<Vec<Value>> {
        let column = self.column;
        let mut result: Vec<Value> = Vec::new();
        for (i, v) in column.iter().enumerate() {
            if let Some(valids) = valids {
                if !valids.get_bit(i) {
                    result.push(Value::Null);
                    continue;
                }
            }
            match std::str::from_utf8(v) {
                Ok(v) => match serde_json::from_str::<Value>(v) {
                    Ok(v) => result.push(v),
                    Err(e) => {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "Error parsing JSON: {}",
                            e
                        )));
                    }
                },
                Err(e) => {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "Error parsing JSON: {}",
                        e
                    )));
                }
            }
        }
        Ok(result)
    }

    fn serialize_json_object_suppress_error(
        &self,
        _format: &FormatSettings,
    ) -> Result<Vec<Option<Value>>> {
        let result: Vec<Option<Value>> = self
            .column
            .iter()
            .map(|v| match std::str::from_utf8(v) {
                Ok(v) => match serde_json::from_str::<Value>(v) {
                    Ok(v) => Some(v),
                    Err(_) => None,
                },
                Err(_) => None,
            })
            .collect();
        Ok(result)
    }
}
