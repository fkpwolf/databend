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

#[derive(Clone)]
pub struct StructSerializer<'a> {
    pub inners: Vec<TypeSerializerImpl<'a>>,
    pub(crate) column: &'a ColumnRef,
}

impl<'a> TypeSerializer<'a> for StructSerializer<'a> {
    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let column = self.column;
        let mut result = Vec::with_capacity(column.len());
        for i in 0..column.len() {
            let val = column.get(i);
            let s = serde_json::to_value(val)?;
            result.push(s);
        }
        Ok(result)
    }
}
