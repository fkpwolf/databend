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

mod iterator;
mod mutable;
mod transform;

use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::compute::cast::binary_to_large_binary;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::types::Index;
use common_arrow::ArrayRef;
use common_io::prelude::BinaryWrite;
pub use iterator::*;
pub use mutable::*;

use crate::prelude::*;

// TODO adaptive offset
#[derive(Clone)]
pub struct StringColumn {
    offsets: Buffer<i64>,
    values: Buffer<u8>,
}

impl From<LargeBinaryArray> for StringColumn {
    fn from(array: LargeBinaryArray) -> Self {
        Self {
            offsets: array.offsets().clone(),
            values: array.values().clone(),
        }
    }
}

impl StringColumn {
    pub fn new(array: LargeBinaryArray) -> Self {
        Self {
            offsets: array.offsets().clone(),
            values: array.values().clone(),
        }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        let arrow_type = array.data_type();
        if arrow_type == &ArrowType::Binary {
            let arr = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            let arr = binary_to_large_binary(arr, ArrowType::LargeBinary);
            return Self::new(arr);
        }

        if arrow_type == &ArrowType::Utf8 {
            let arr = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            let offsets = arr
                .offsets()
                .iter()
                .map(|x| *x as i64)
                .collect::<Buffer<_>>();
            return Self {
                offsets,
                values: arr.values().clone(),
            };
        }

        if arrow_type == &ArrowType::LargeUtf8 {
            let arr = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            return Self {
                offsets: arr.offsets().clone(),
                values: arr.values().clone(),
            };
        }

        Self::new(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .clone(),
        )
    }

    /// construct StringColumn from unchecked data
    /// # Safety
    /// just like BinaryArray::from_data_unchecked, as follows
    /// * `offsets` MUST be monotonically increasing
    /// # Panics
    /// This function panics if:
    /// * The last element of `offsets` is different from `values.len()`.
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub unsafe fn from_data_unchecked(offsets: Buffer<i64>, values: Buffer<u8>) -> Self {
        Self { offsets, values }
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        // soundness: the invariant of the function
        let start = self.offsets.get_unchecked(i).to_usize();
        let end = self.offsets.get_unchecked(i + 1).to_usize();
        // soundness: the invariant of the struct
        self.values.get_unchecked(start..end)
    }

    pub fn to_binary_array(&self) -> BinaryArray<i64> {
        unsafe {
            BinaryArray::from_data_unchecked(
                ArrowType::LargeBinary,
                self.offsets.clone(),
                self.values.clone(),
                None,
            )
        }
    }

    #[inline]
    pub fn size_at_index(&self, i: usize) -> usize {
        let offset = self.offsets[i];
        let offset_1 = self.offsets[i + 1];
        (offset_1 - offset).to_usize()
    }

    pub fn values(&self) -> &[u8] {
        self.values.as_slice()
    }

    pub fn offsets(&self) -> &[i64] {
        self.offsets.as_slice()
    }
}

impl Column for StringColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypeImpl {
        StringType::new_impl()
    }

    fn column_type_name(&self) -> String {
        "String".to_string()
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn memory_size(&self) -> usize {
        self.values.len() + self.offsets.len() * std::mem::size_of::<i64>()
    }

    fn as_arrow_array(&self, logical_type: DataTypeImpl) -> ArrayRef {
        Box::new(LargeBinaryArray::from_data(
            logical_type.arrow_type(),
            self.offsets.clone(),
            self.values.clone(),
            None,
        ))
    }

    fn arc(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        let offsets = unsafe { self.offsets.clone().slice_unchecked(offset, length + 1) };

        Arc::new(Self {
            offsets,
            values: self.values.clone(),
        })
    }

    fn scatter(&self, indices: &[usize], scattered_size: usize) -> Vec<ColumnRef> {
        scatter_scalar_column(self, indices, scattered_size)
    }

    fn filter(&self, filter: &BooleanColumn) -> ColumnRef {
        filter_scalar_column(self, filter)
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        replicate_scalar_column(self, offsets)
    }

    fn convert_full_column(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn get(&self, index: usize) -> DataValue {
        let start = self.offsets[index].to_usize();
        let end = self.offsets[index + 1].to_usize();

        // soundness: the invariant of the struct
        let str = unsafe { self.values.get_unchecked(start..end) };
        DataValue::String(str.to_vec())
    }

    fn serialize(&self, vec: &mut Vec<u8>, row: usize) {
        let value = self.get_data(row);
        BinaryWrite::write_binary(vec, value).unwrap()
    }
}

impl ScalarColumn for StringColumn {
    type Builder = MutableStringColumn;
    type OwnedItem = Vec<u8>;
    type RefItem<'a> = &'a [u8];
    type Iterator<'a> = StringValueIter<'a>;

    #[inline]
    fn get_data(&self, idx: usize) -> Self::RefItem<'_> {
        let start = self.offsets[idx].to_usize();
        let end = self.offsets[idx + 1].to_usize();

        // soundness: the invariant of the struct
        unsafe { self.values.get_unchecked(start..end) }
    }

    fn scalar_iter(&self) -> Self::Iterator<'_> {
        StringValueIter::new(self)
    }
}

impl std::fmt::Debug for StringColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let iter = self.iter().map(String::from_utf8_lossy);
        let head = "StringColumn";
        display_fmt(iter, head, self.len(), self.data_type_id(), f)
    }
}
