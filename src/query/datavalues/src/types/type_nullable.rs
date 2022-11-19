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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::ErrorCode;
use common_exception::Result;
use rand::prelude::*;

use super::data_type::DataType;
use super::data_type::DataTypeImpl;
use super::type_id::TypeID;
use crate::prelude::*;
use crate::serializations::NullableSerializer;
use crate::serializations::TypeSerializerImpl;

#[derive(Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct NullableType {
    inner: Box<DataTypeImpl>,
}

impl NullableType {
    pub fn new_impl(inner: DataTypeImpl) -> DataTypeImpl {
        DataTypeImpl::Nullable(Self::create(inner))
    }

    pub fn create(inner: DataTypeImpl) -> Self {
        debug_assert!(
            inner.can_inside_nullable(),
            "{} can't be inside of nullable.",
            inner.name()
        );
        NullableType {
            inner: Box::new(inner),
        }
    }

    pub fn inner_type(&self) -> &DataTypeImpl {
        &self.inner
    }
}

impl DataType for NullableType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Nullable
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        format!("Nullable({})", self.inner.name())
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn can_inside_nullable(&self) -> bool {
        false
    }

    fn default_value(&self) -> DataValue {
        DataValue::Null
    }

    fn random_value(&self) -> DataValue {
        let mut rng = rand::rngs::SmallRng::from_entropy();
        let p = rng.gen_bool(0.5);
        // half possibility to be null
        if p {
            self.inner.random_value()
        } else {
            DataValue::Null
        }
    }

    fn arrow_type(&self) -> ArrowType {
        self.inner.arrow_type()
    }

    fn custom_arrow_meta(&self) -> Option<BTreeMap<String, String>> {
        self.inner.custom_arrow_meta()
    }

    fn create_serializer_inner<'a>(&self, column: &'a ColumnRef) -> Result<TypeSerializerImpl<'a>> {
        let column: &NullableColumn = Series::check_get(column)?;
        Ok(NullableSerializer {
            validity: column.ensure_validity(),
            inner: Box::new(self.inner.create_serializer(column.inner())?),
        }
        .into())
    }

    fn create_deserializer(&self, capacity: usize) -> TypeDeserializerImpl {
        NullableDeserializer {
            inner: Box::new(self.inner.create_deserializer(capacity)),
            bitmap: MutableBitmap::with_capacity(capacity),
        }
        .into()
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutableNullableColumn::new(
            self.inner.create_mutable(capacity),
            DataTypeImpl::Nullable(self.clone()),
        ))
    }

    fn create_constant_column(
        &self,
        data: &DataValue,
        size: usize,
    ) -> common_exception::Result<ColumnRef> {
        let mut bitmap = MutableBitmap::with_capacity(1);

        if self.inner.data_type_id() == TypeID::Null {
            return Ok(Arc::new(NullColumn::new(size)));
        }
        if self.inner.data_type_id() == TypeID::Nullable {
            return Result::Err(ErrorCode::BadDataValueType(
                "Nullable type can't be inside nullable type".to_string(),
            ));
        }
        let data = if data.is_null() {
            bitmap.extend_constant(size, false);
            self.inner.default_value()
        } else {
            bitmap.extend_constant(size, true);
            data.clone()
        };
        let column = self.inner.create_constant_column(&data, size)?;
        Ok(NullableColumn::wrap_inner(column, Some(bitmap.into())))
    }

    fn create_column(&self, data: &[DataValue]) -> common_exception::Result<ColumnRef> {
        let mut res = Vec::with_capacity(data.len());
        let mut bitmap = MutableBitmap::with_capacity(data.len());

        for v in data {
            if v.is_null() {
                bitmap.push(false);
                res.push(self.inner.default_value());
            } else {
                bitmap.push(true);
                res.push(v.clone());
            }
        }
        let column = self.inner.create_column(&res)?;
        Ok(NullableColumn::wrap_inner(column, Some(bitmap.into())))
    }
}

impl std::fmt::Debug for NullableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
