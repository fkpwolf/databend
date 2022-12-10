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

use std::any::Any;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::ArrayRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;
use crate::BooleanColumn;
use crate::DataTypeImpl;
use crate::DataValue;
use crate::NullColumn;
use crate::TypeID;

pub type ColumnRef = Arc<dyn Column>;
pub trait Column: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    /// Type of data that column contains. It's an underlying physical type:
    /// Int32 for Date, Int64 for Timestamp, so on.
    fn data_type_id(&self) -> TypeID {
        self.data_type().data_type_id()
    }

    fn data_type(&self) -> DataTypeImpl;

    fn column_meta(&self) -> ColumnMeta {
        ColumnMeta::Simple
    }

    fn column_type_name(&self) -> String;

    fn is_nullable(&self) -> bool {
        false
    }

    fn is_null(&self) -> bool {
        false
    }

    fn is_const(&self) -> bool {
        false
    }

    fn len(&self) -> usize;
    /// whether the array is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn null_at(&self, _row: usize) -> bool {
        false
    }

    /// If the only value column can contain is NULL.
    fn only_null(&self) -> bool {
        false
    }

    /// Returns (is_all_null,  Option bitmap)
    fn validity(&self) -> (bool, Option<&Bitmap>) {
        (false, None)
    }

    fn memory_size(&self) -> usize;
    fn arc(&self) -> ColumnRef;
    fn as_arrow_array(&self, logical_type: DataTypeImpl) -> ArrayRef;
    fn slice(&self, offset: usize, length: usize) -> ColumnRef;

    fn filter(&self, filter: &BooleanColumn) -> ColumnRef;

    /// scatter() partitions the input array into multiple arrays.
    /// indices: a slice whose length is the same as the array.
    /// The element of indices indicates which group the corresponding row
    /// in the input array belongs to.
    /// scattered_size: the number of partitions
    ///
    /// Example: if the input array has four rows [1, 2, 3, 4] and
    /// _indices = [0, 1, 0, 1] and _scatter_size = 2,
    /// then the output would be a vector of two arrays: [1, 3] and [2, 4].
    fn scatter(&self, indices: &[usize], scattered_size: usize) -> Vec<ColumnRef>;

    // Copies each element according offsets parameter.
    // (i-th element should be copied offsets[i] - offsets[i - 1] times.)
    fn replicate(&self, offsets: &[usize]) -> ColumnRef;

    fn convert_full_column(&self) -> ColumnRef;

    /// # Safety
    /// Assumes that the `index` is smaller than size.
    fn get(&self, index: usize) -> DataValue;

    fn get_checked(&self, index: usize) -> Result<DataValue> {
        if index >= self.len() {
            return Err(ErrorCode::BadDataArrayLength(format!(
                "Index out of bounds: {}, col size: {}",
                index,
                self.len()
            )));
        }
        Ok(self.get(index))
    }

    /// # Safety
    /// Assumes that the `index` is smaller than size.
    fn get_u64(&self, index: usize) -> Result<u64> {
        let value = self.get(index);
        DFTryFrom::try_from(&value)
    }

    /// # Safety
    /// Assumes that the `index` is smaller than size.
    fn get_i64(&self, index: usize) -> Result<i64> {
        let value = self.get(index);
        DFTryFrom::try_from(&value)
    }

    fn get_f64(&self, index: usize) -> Result<f64> {
        let value = self.get(index);
        DFTryFrom::try_from(&value)
    }

    /// # Safety
    /// Assumes that the `index` is smaller than size.
    fn get_bool(&self, index: usize) -> Result<bool> {
        let value = self.get(index);
        DFTryFrom::try_from(&value)
    }

    /// # Safety
    /// Assumes that the `index` is smaller than size.
    fn get_string(&self, index: usize) -> Result<Vec<u8>> {
        let value = self.get(index);
        DFTryFrom::try_from(value)
    }

    /// Visit each row value of Column
    fn for_each<F>(&self, f: F)
    where
        Self: Sized,
        F: Fn(usize, DataValue),
    {
        (0..self.len()).for_each(|i| f(i, self.get(i)))
    }

    /// Apply binary mode function to each element of the column.
    /// WARN: Can't use `&mut [Vec<u8>]` because it has performance drawback.
    /// Refer: https://github.com/rust-lang/rust-clippy/issues/8334
    // not nullable col1                 nullable col2 (first byte to indicate null or not)
    // │                                 │
    // │                                 │
    // ▼                                 ▼
    // ┌──────────┬──────────┬───────────┬───────────┬───────────┬───────────┬─────────┬─────────┐
    // │   1byte  │   1byte  │    1byte  │    1byte  │   1byte   │    1byte  │   1byte │   1byte │ ....
    // └──────────┴──────────┴───────────┴───────────┴───────────┴───────────┴─────────┴─────────┘
    //  ▲                                ▲           ▲                                           ▲
    //  │                                │           │                                           │
    //  │        Binary Datas            │ null sign │              Binary Datas                 │
    //  │                                │           │                                           │
    //  └────────────────────────────────┘           └──────────────────────────────────────────►┘
    fn serialize(&self, vec: &mut Vec<u8>, row: usize);
}

pub trait IntoColumn {
    fn into_column(self) -> ColumnRef;
    fn into_nullable_column(self) -> ColumnRef;
}

impl<A> IntoColumn for A
where A: AsRef<dyn Array>
{
    fn into_column(self) -> ColumnRef {
        use TypeID::*;
        let data_type: DataTypeImpl = from_arrow_type(self.as_ref().data_type());
        match data_type.data_type_id() {
            // arrow type has no nullable type
            Nullable => unimplemented!(),
            Null => Arc::new(NullColumn::from_arrow_array(self.as_ref())),
            Boolean => Arc::new(BooleanColumn::from_arrow_array(self.as_ref())),
            UInt8 => Arc::new(UInt8Column::from_arrow_array(self.as_ref())),
            UInt16 => Arc::new(UInt16Column::from_arrow_array(self.as_ref())),
            UInt32 => Arc::new(UInt32Column::from_arrow_array(self.as_ref())),
            UInt64 => Arc::new(UInt64Column::from_arrow_array(self.as_ref())),
            Int8 => Arc::new(Int8Column::from_arrow_array(self.as_ref())),
            Int16 => Arc::new(Int16Column::from_arrow_array(self.as_ref())),
            Int32 | Date => Arc::new(Int32Column::from_arrow_array(self.as_ref())),
            Int64 | Interval | Timestamp => Arc::new(Int64Column::from_arrow_array(self.as_ref())),
            Float32 => Arc::new(Float32Column::from_arrow_array(self.as_ref())),
            Float64 => Arc::new(Float64Column::from_arrow_array(self.as_ref())),
            Array => Arc::new(ArrayColumn::from_arrow_array(self.as_ref())),
            Struct => Arc::new(StructColumn::from_arrow_array(self.as_ref())),
            String => Arc::new(StringColumn::from_arrow_array(self.as_ref())),
            Variant => Arc::new(VariantColumn::from_arrow_array(self.as_ref())),
            VariantArray => Arc::new(VariantColumn::from_arrow_array(self.as_ref())),
            VariantObject => Arc::new(VariantColumn::from_arrow_array(self.as_ref())),
        }
    }

    fn into_nullable_column(self) -> ColumnRef {
        let validity = self.as_ref().validity().cloned();
        let column = self.as_ref().into_column();
        NullableColumn::wrap_inner(column, validity)
    }
}

pub fn display_helper<T: std::fmt::Display, I: IntoIterator<Item = T>>(iter: I) -> Vec<String> {
    iter.into_iter().map(|x| x.to_string()).collect::<Vec<_>>()
}

pub fn display_fmt<T: std::fmt::Display, I: IntoIterator<Item = T>>(
    iter: I,
    head: &str,
    len: usize,
    typeid: TypeID,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    let result = display_helper(iter);
    write!(
        f,
        "{} \t typeid: {:?}\t len: {}\t data: [{}]",
        head,
        typeid,
        len,
        result.join(", ")
    )
}

macro_rules! fmt_dyn {
    ($column:expr, $ty:ty, $f:expr) => {{
        let mut f = |x: &$ty| x.fmt($f);
        let column = $column.as_any().downcast_ref::<$ty>().unwrap();
        f(column)?
    }};
}

impl std::fmt::Debug for dyn Column + '_ {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dt = self.data_type().data_type_id();
        let col = self.convert_full_column();
        with_match_primitive_type_id!(dt, |$T| {
            fmt_dyn!(col, PrimitiveColumn<$T>, f)
        }, {
            use crate::types::type_id::TypeID::*;
            match dt {
                Null => {
                    fmt_dyn!(col, NullColumn, f)
                }
                Nullable => {
                    fmt_dyn!(col, NullableColumn, f)
                },
                Boolean => {
                    fmt_dyn!(col, BooleanColumn, f)
                },
                String => {
                    fmt_dyn!(col, StringColumn, f)
                },
                Array => {
                    fmt_dyn!(col, ArrayColumn, f)
                },
                Struct => {
                    fmt_dyn!(col, StructColumn, f)
                },
                Variant | VariantArray | VariantObject => {
                    fmt_dyn!(col, VariantColumn, f)
                }
                _ => {
                    unimplemented!()
                }
            }
        });

        Ok(())
    }
}
