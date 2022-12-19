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

use core::ops::Not;
use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::ArrayRef;

use crate::prelude::*;

mod iterator;
mod mutable;

pub use iterator::*;
pub use mutable::*;

#[derive(Clone)]
pub struct BooleanColumn {
    values: Bitmap,
}

impl From<BooleanArray> for BooleanColumn {
    fn from(array: BooleanArray) -> Self {
        Self::new(array)
    }
}

impl BooleanColumn {
    pub fn new(array: BooleanArray) -> Self {
        Self {
            values: array.values().clone(),
        }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self::new(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .clone(),
        )
    }

    pub fn from_arrow_data(values: Bitmap) -> Self {
        Self::from_arrow_array(&BooleanArray::try_new(ArrowType::Boolean, values, None).unwrap())
    }

    pub fn values(&self) -> &Bitmap {
        &self.values
    }

    pub fn neg(&self) -> Self {
        Self::from_arrow_data(Not::not(&self.values))
    }
}

impl Column for BooleanColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypeImpl {
        BooleanType::new_impl()
    }

    fn column_type_name(&self) -> String {
        "Boolean".to_string()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn memory_size(&self) -> usize {
        self.values.as_slice().0.len()
    }

    fn as_arrow_array(&self, logical_type: DataTypeImpl) -> ArrayRef {
        let array =
            BooleanArray::try_new(logical_type.arrow_type(), self.values.clone(), None).unwrap();
        Box::new(array)
    }

    fn arc(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe {
            Arc::new(Self {
                values: self.values.clone().slice_unchecked(offset, length),
            })
        }
    }

    fn filter(&self, filter: &BooleanColumn) -> ColumnRef {
        if self.values().unset_bits() == 0 {
            let values = self
                .values
                .clone()
                .slice(0, filter.len() - filter.values().unset_bits());
            return Arc::new(BooleanColumn::from_arrow_data(values));
        }
        filter_scalar_column(self, filter)
    }

    fn scatter(&self, indices: &[usize], scattered_size: usize) -> Vec<ColumnRef> {
        scatter_scalar_column(self, indices, scattered_size)
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        replicate_scalar_column(self, offsets)
    }

    fn convert_full_column(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn get(&self, index: usize) -> DataValue {
        DataValue::Boolean(self.values.get_bit(index))
    }

    fn serialize(&self, vec: &mut Vec<u8>, row: usize) {
        vec.push(self.values.get_bit(row) as u8);
    }
}

impl ScalarColumn for BooleanColumn {
    type Builder = MutableBooleanColumn;
    type OwnedItem = bool;
    type RefItem<'a> = bool;
    type Iterator<'a> = BitmapValuesIter<'a>;

    #[inline]
    fn get_data(&self, idx: usize) -> Self::RefItem<'_> {
        self.values.get_bit(idx)
    }

    fn scalar_iter(&self) -> Self::Iterator<'_> {
        self.iter()
    }

    fn from_slice(data: &[Self::RefItem<'_>]) -> Self {
        let bitmap = MutableBitmap::from_iter(data.as_ref().iter().cloned());
        BooleanColumn {
            values: bitmap.into(),
        }
    }

    fn from_iterator<'a>(it: impl Iterator<Item = Self::RefItem<'a>>) -> Self {
        let bitmap = MutableBitmap::from_iter(it);
        BooleanColumn {
            values: bitmap.into(),
        }
    }

    fn from_owned_iterator(it: impl Iterator<Item = Self::OwnedItem>) -> Self {
        let bitmap = match it.size_hint() {
            (_, Some(_)) => unsafe { MutableBitmap::from_trusted_len_iter_unchecked(it) },
            (_, None) => MutableBitmap::from_iter(it),
        };
        BooleanColumn {
            values: bitmap.into(),
        }
    }
}

impl std::fmt::Debug for BooleanColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let iter = self.iter().map(|x| if x { "true" } else { "false" });
        let head = "BooleanColumn";
        display_fmt(iter, head, self.len(), self.data_type_id(), f)
    }
}
