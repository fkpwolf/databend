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

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::Result;

use super::data_type::DataType;
use crate::prelude::*;
use crate::serializations::NullSerializer;
use crate::serializations::TypeSerializerImpl;

#[derive(Default, Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct NullType {}

impl NullType {
    pub fn new_impl() -> DataTypeImpl {
        DataTypeImpl::Null(Self {})
    }
}

impl DataType for NullType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Null
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "Null".to_string()
    }

    fn can_inside_nullable(&self) -> bool {
        false
    }

    // it's nothing, so we can't create any default value
    fn default_value(&self) -> DataValue {
        DataValue::Null
    }

    fn create_constant_column(
        &self,
        _data: &DataValue,
        size: usize,
    ) -> common_exception::Result<ColumnRef> {
        Ok(Arc::new(NullColumn::new(size)))
    }

    // NullType must inside nullable
    fn arrow_type(&self) -> ArrowType {
        ArrowType::Null
    }

    fn create_serializer_inner<'a>(&self, col: &'a ColumnRef) -> Result<TypeSerializerImpl<'a>> {
        Ok(NullSerializer { size: col.len() }.into())
    }

    fn create_deserializer(&self, _capacity: usize) -> TypeDeserializerImpl {
        NullDeserializer {
            builder: MutableNullColumn::default(),
        }
        .into()
    }

    fn create_column(&self, data: &[DataValue]) -> common_exception::Result<ColumnRef> {
        Ok(Arc::new(NullColumn::new(data.len())))
    }

    fn create_mutable(&self, _capacity: usize) -> Box<dyn MutableColumn> {
        Box::<MutableNullColumn>::default()
    }
}

impl std::fmt::Debug for NullType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
