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

use crate::serializations::TypeSerializer;
use crate::serializations::TypeSerializerImpl;

#[derive(Clone)]
pub struct NullableSerializer<'a> {
    pub validity: &'a Bitmap,
    pub inner: Box<TypeSerializerImpl<'a>>,
}

impl<'a> TypeSerializer<'a> for NullableSerializer<'a> {
    fn serialize_json_values(&self, format: &FormatSettings) -> Result<Vec<Value>> {
        let mut res = self.inner.serialize_json_values(format)?;
        let validity = self.validity;

        (0..validity.len()).for_each(|row| {
            if !validity.get_bit(row) {
                res[row] = Value::Null;
            }
        });
        Ok(res)
    }
}
