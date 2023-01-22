// Copyright 2022 Datafuse Labs.
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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::Result;

use crate::types::array::ArrayColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::AnyType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::with_number_mapped_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Scalar;
use crate::TypeDeserializer;
use crate::Value;

impl DataBlock {
    pub fn scatter<I>(&self, indices: &[I], scatter_size: usize) -> Result<Vec<Self>>
    where I: common_arrow::arrow::types::Index {
        if indices.is_empty() {
            let mut result = Vec::with_capacity(scatter_size);
            result.push(self.clone());
            for _ in 1..scatter_size {
                result.push(self.slice(0..0));
            }
            return Ok(result);
        }

        let scattered_columns: Vec<Vec<BlockEntry>> = self
            .columns()
            .iter()
            .map(|entry| match &entry.value {
                Value::Scalar(s) => {
                    Column::scatter_repeat_scalars::<I>(s, &entry.data_type, indices, scatter_size)
                        .into_iter()
                        .map(|value| BlockEntry {
                            data_type: entry.data_type.clone(),
                            value: Value::Column(value),
                        })
                        .collect()
                }
                Value::Column(c) => c
                    .scatter(&entry.data_type, indices, scatter_size)
                    .into_iter()
                    .map(|value| BlockEntry {
                        data_type: entry.data_type.clone(),
                        value: Value::Column(value),
                    })
                    .collect(),
            })
            .collect();

        let scattered_chunks = (0..scatter_size)
            .map(|scatter_idx| {
                let chunk_columns: Vec<BlockEntry> = scattered_columns
                    .iter()
                    .map(|entry| entry[scatter_idx].clone())
                    .collect();
                let num_rows = if chunk_columns.is_empty() {
                    indices
                        .iter()
                        .filter(|&i| i.to_usize() == scatter_idx)
                        .count()
                } else {
                    chunk_columns[0].value.as_column().unwrap().len()
                };
                DataBlock::new(chunk_columns, num_rows)
            })
            .collect();

        Ok(scattered_chunks)
    }
}

impl Column {
    pub fn scatter_repeat_scalars<I>(
        scalar: &Scalar,
        data_type: &DataType,
        indices: &[I],
        scatter_size: usize,
    ) -> Vec<Self>
    where
        I: common_arrow::arrow::types::Index,
    {
        let mut vs = vec![0usize; scatter_size];
        for index in indices {
            vs[index.to_usize()] += 1;
        }
        vs.iter()
            .map(|count| ColumnBuilder::repeat(&scalar.as_ref(), *count, data_type).build())
            .collect()
    }

    pub fn scatter<I>(
        &self,
        data_type: &DataType,
        indices: &[I],
        scatter_size: usize,
    ) -> Vec<Self>
    where
        I: common_arrow::arrow::types::Index,
    {
        let length = indices.len();
        match self {
            Column::Null { .. } => {
                Self::scatter_repeat_scalars::<I>(&Scalar::Null, data_type, indices, scatter_size)
            }
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) => Self::scatter_scalars::<NumberType<NUM_TYPE>, _>(
                    values,
                    Vec::with_capacity(length),
                    indices,
                    scatter_size
                ),
            }),
            Column::EmptyArray { .. } => Self::scatter_repeat_scalars::<I>(
                &Scalar::EmptyArray,
                data_type,
                indices,
                scatter_size,
            ),
            Column::Boolean(bm) => Self::scatter_scalars::<BooleanType, _>(
                bm,
                MutableBitmap::with_capacity(length),
                indices,
                scatter_size,
            ),
            Column::String(column) => Self::scatter_scalars::<StringType, _>(
                column,
                StringColumnBuilder::with_capacity(length, 0),
                indices,
                scatter_size,
            ),
            Column::Timestamp(column) => Self::scatter_scalars::<TimestampType, _>(
                column,
                Vec::with_capacity(length),
                indices,
                scatter_size,
            ),
            Column::Date(column) => Self::scatter_scalars::<DateType, _>(
                column,
                Vec::with_capacity(length),
                indices,
                scatter_size,
            ),
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(length + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    column
                        .values
                        .data_type()
                        .create_deserializer(length)
                        .finish_to_column(),
                );
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::scatter_scalars::<ArrayType<AnyType>, _>(
                    column,
                    builder,
                    indices,
                    scatter_size,
                )
            }
            Column::Nullable(c) => {
                let columns = c.column.scatter(data_type, indices, scatter_size);
                let validitys = Self::scatter_scalars::<BooleanType, _>(
                    &c.validity,
                    MutableBitmap::with_capacity(length),
                    indices,
                    scatter_size,
                );
                columns
                    .iter()
                    .zip(&validitys)
                    .map(|(column, validity)| {
                        Column::Nullable(Box::new(NullableColumn {
                            column: column.clone(),
                            validity: BooleanType::try_downcast_column(validity).unwrap(),
                        }))
                    })
                    .collect()
            }
            Column::Tuple { fields, .. } => {
                let fields_vs: Vec<Vec<Column>> = fields
                    .iter()
                    .map(|c| c.scatter(data_type, indices, scatter_size))
                    .collect();

                let mut res = Vec::with_capacity(scatter_size);

                for s in 0..scatter_size {
                    let mut fields = Vec::with_capacity(fields.len());
                    for col in &fields_vs {
                        fields.push(col[s].clone());
                    }
                    res.push(Column::Tuple {
                        len: fields.first().map_or(0, |f| f.len()),
                        fields,
                    });
                }
                res
            }
            Column::Variant(column) => Self::scatter_scalars::<VariantType, _>(
                column,
                StringColumnBuilder::with_capacity(length, 0),
                indices,
                scatter_size,
            ),
        }
    }

    fn scatter_scalars<T: ValueType, I>(
        col: &T::Column,
        builder: T::ColumnBuilder,
        indices: &[I],
        scatter_size: usize,
    ) -> Vec<Self>
    where
        I: common_arrow::arrow::types::Index,
    {
        let mut builders: Vec<T::ColumnBuilder> =
            std::iter::repeat(builder).take(scatter_size).collect();

        indices
            .iter()
            .zip(T::iter_column(col))
            .for_each(|(index, item)| {
                T::push_item(&mut builders[index.to_usize()], item);
            });
        builders
            .into_iter()
            .map(|b| T::upcast_column(T::build_column(b)))
            .collect()
    }
}
