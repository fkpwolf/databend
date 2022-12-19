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

use common_arrow::arrow::array::*;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::compute::cast::CastOptions;
use common_arrow::arrow::compute::cast::{self};
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::offset::OffsetsBuffer;
use common_arrow::arrow::types::Index;
use common_arrow::ArrayRef;
use common_io::prelude::BinaryWrite;

use crate::prelude::*;

mod iterator;
mod mutable;

pub use iterator::*;
pub use mutable::*;

type LargeListArray = ListArray<i64>;

#[derive(Clone)]
pub struct ArrayColumn {
    data_type: DataTypeImpl,
    offsets: Buffer<i64>,
    values: ColumnRef,
}

impl ArrayColumn {
    pub fn new(array: LargeListArray) -> Self {
        let ty = array.data_type();
        let (data_type, values) = if let ArrowType::LargeList(f) = ty {
            let inner_type = from_arrow_field(f);
            let values = if inner_type.is_nullable() {
                array.values().clone().into_nullable_column()
            } else {
                array.values().clone().into_column()
            };
            let data_type = ArrayType::new_impl(inner_type);
            (data_type, values)
        } else {
            unreachable!()
        };

        Self {
            data_type,
            offsets: array.offsets().clone().into_inner(),
            values,
        }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        let cast_options = CastOptions {
            wrapped: true,
            partial: true,
        };

        match array.data_type() {
            ArrowType::List(f) => {
                let array = cast::cast(array, &ArrowType::LargeList(f.clone()), cast_options)
                    .expect("list to large list cast should be ok");
                Self::from_arrow_array(array.as_ref())
            }
            _ => Self::new(
                array
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .unwrap()
                    .clone(),
            ),
        }
    }

    pub fn from_data(data_type: DataTypeImpl, offsets: Buffer<i64>, values: ColumnRef) -> Self {
        Self {
            data_type,
            offsets,
            values,
        }
    }

    #[inline]
    pub fn size_at_index(&self, i: usize) -> usize {
        let offset = self.offsets[i];
        let offset_1 = self.offsets[i + 1];
        (offset_1 - offset).to_usize()
    }

    pub fn values(&self) -> &ColumnRef {
        &self.values
    }

    pub fn offsets(&self) -> &[i64] {
        self.offsets.as_slice()
    }
}

impl Column for ArrayColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypeImpl {
        self.data_type.clone()
    }

    fn column_type_name(&self) -> String {
        "Array".to_string()
    }

    fn column_meta(&self) -> ColumnMeta {
        let data_type: ArrayType = self.data_type.clone().try_into().unwrap();
        ColumnMeta::Array {
            inner_type: data_type.inner_type().clone(),
        }
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn memory_size(&self) -> usize {
        self.values.memory_size() + self.offsets.len() * std::mem::size_of::<i64>()
    }

    fn as_arrow_array(&self, data_type: DataTypeImpl) -> ArrayRef {
        let arrow_type = data_type.arrow_type();
        if let ArrowType::LargeList(ref f) = arrow_type {
            let inner_f = from_arrow_field(f.as_ref());
            let array = self.values.as_arrow_array(inner_f);
            Box::new(
                LargeListArray::try_new(
                    arrow_type,
                    unsafe { OffsetsBuffer::new_unchecked(self.offsets.clone()) },
                    array,
                    None,
                )
                .unwrap(),
            )
        } else {
            unreachable!()
        }
    }

    fn arc(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        unsafe {
            let offsets = self.offsets.clone().slice_unchecked(offset, length + 1);
            Arc::new(Self {
                data_type: self.data_type.clone(),
                offsets,
                values: self.values.clone(),
            })
        }
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
        let offset = self.offsets[index] as usize;
        let length = self.size_at_index(index);
        let values = (offset..offset + length)
            .map(|i| self.values.get(i))
            .collect();
        DataValue::Array(values)
    }

    fn serialize(&self, vec: &mut Vec<u8>, row: usize) {
        let offset = self.offsets[row] as usize;
        let length = self.size_at_index(row);

        BinaryWrite::write_uvarint(vec, length as u64).unwrap();
        for row in offset..offset + length {
            self.values.serialize(vec, row);
        }
    }
}

impl ScalarColumn for ArrayColumn {
    type Builder = MutableArrayColumn;
    type OwnedItem = ArrayValue;
    type RefItem<'a> = <ArrayValue as Scalar>::RefType<'a>;
    type Iterator<'a> = ArrayValueIter<'a>;

    #[inline]
    fn get_data(&self, idx: usize) -> Self::RefItem<'_> {
        ArrayValueRef::Indexed { column: self, idx }
    }

    fn scalar_iter(&self) -> Self::Iterator<'_> {
        ArrayValueIter::new(self)
    }
}

impl std::fmt::Debug for ArrayColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut data = Vec::with_capacity(self.len());
        for idx in 0..self.len() {
            data.push(format!("{:?}", self.get(idx)));
        }
        let head = "ArrayColumn";
        let iter = data.iter();
        display_fmt(iter, head, self.len(), self.data_type_id(), f)
    }
}
