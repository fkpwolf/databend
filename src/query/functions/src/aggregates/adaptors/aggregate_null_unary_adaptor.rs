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

use std::alloc::Layout;
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::BinaryWrite;

use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;

#[derive(Clone)]
pub struct AggregateNullUnaryAdaptor<const NULLABLE_RESULT: bool> {
    nested: AggregateFunctionRef,
    size_of_data: usize,
}

impl<const NULLABLE_RESULT: bool> AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    pub fn create(nested: AggregateFunctionRef) -> AggregateFunctionRef {
        let size_of_data = if NULLABLE_RESULT {
            let layout = nested.state_layout();
            layout.size()
        } else {
            0
        };
        Arc::new(Self {
            nested,
            size_of_data,
        })
    }

    #[inline]
    pub fn set_flag(&self, place: StateAddr, flag: u8) {
        if NULLABLE_RESULT {
            let c = place.next(self.size_of_data).get::<u8>();
            *c = flag;
        }
    }

    #[inline]
    pub fn init_flag(&self, place: StateAddr) {
        if NULLABLE_RESULT {
            let c = place.next(self.size_of_data).get::<u8>();
            *c = 0;
        }
    }

    #[inline]
    pub fn get_flag(&self, place: StateAddr) -> u8 {
        if NULLABLE_RESULT {
            let c = place.next(self.size_of_data).get::<u8>();
            *c
        } else {
            1
        }
    }
}

impl<const NULLABLE_RESULT: bool> AggregateFunction for AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    fn name(&self) -> &str {
        "AggregateNullUnaryAdaptor"
    }

    fn return_type(&self) -> Result<DataTypeImpl> {
        match NULLABLE_RESULT {
            true => Ok(wrap_nullable(&self.nested.return_type()?)),
            false => Ok(self.nested.return_type()?),
        }
    }

    #[inline]
    fn init_state(&self, place: StateAddr) {
        self.init_flag(place);
        self.nested.init_state(place);
    }

    #[inline]
    fn state_layout(&self) -> Layout {
        let layout = self.nested.state_layout();
        let add = usize::from(NULLABLE_RESULT);
        Layout::from_size_align(layout.size() + add, layout.align()).unwrap()
    }

    #[inline]
    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let mut validity = validity.cloned();
        let col = &columns[0];
        let (all_null, v) = col.validity();
        validity = combine_validities(validity.as_ref(), v);
        let not_null_columns = Series::remove_nullable(col);

        self.nested
            .accumulate(place, &[not_null_columns], validity.as_ref(), input_rows)?;

        if !all_null {
            match validity {
                Some(v) => {
                    if v.unset_bits() != input_rows {
                        self.set_flag(place, 1);
                    }
                }
                None => self.set_flag(place, 1),
            }
        }
        Ok(())
    }

    #[inline]
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[ColumnRef],
        input_rows: usize,
    ) -> Result<()> {
        let col = &columns[0];
        let (all_null, validity) = col.validity();
        let not_null_column = Series::remove_nullable(col);
        let not_null_columns = &[not_null_column];

        if !all_null {
            match validity {
                Some(v) if v.unset_bits() > 0 => {
                    for (valid, (row, place)) in v.iter().zip(places.iter().enumerate()) {
                        if valid {
                            self.set_flag(place.next(offset), 1);
                            self.nested.accumulate_row(
                                place.next(offset),
                                not_null_columns,
                                row,
                            )?;
                        }
                    }
                }
                _ => {
                    self.nested
                        .accumulate_keys(places, offset, not_null_columns, input_rows)?;
                    places
                        .iter()
                        .for_each(|place| self.set_flag(place.next(offset), 1));
                }
            }
        }
        Ok(())
    }

    /// we already have accumulate_keys, so we don't need to implement this
    fn accumulate_row(&self, _place: StateAddr, _columns: &[ColumnRef], _row: usize) -> Result<()> {
        unreachable!()
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        self.nested.serialize(place, writer)?;
        if NULLABLE_RESULT {
            let flag = self.get_flag(place);
            writer.write_scalar(&flag)?;
        }
        Ok(())
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        if NULLABLE_RESULT {
            self.nested
                .deserialize(place, &mut &reader[..reader.len() - 1])?;
            let flag = reader[reader.len() - 1];
            self.set_flag(place, flag);
        } else {
            self.nested.deserialize(place, reader)?;
        }

        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        if self.get_flag(place) == 0 {
            // initial the state to remove the dirty stats
            self.init_state(place);
        }

        if self.get_flag(rhs) == 1 {
            self.set_flag(place, 1);
        }

        self.nested.merge(place, rhs)
    }

    fn merge_result(&self, place: StateAddr, column: &mut dyn MutableColumn) -> Result<()> {
        if NULLABLE_RESULT {
            let builder: &mut MutableNullableColumn = Series::check_get_mutable_column(column)?;
            if self.get_flag(place) == 1 {
                let inner = builder.inner_mut();
                self.nested.merge_result(place, inner.as_mut())?;
                let validity = builder.validity_mut();

                validity.push(true);
            } else {
                builder.append_default();
            }
            Ok(())
        } else {
            self.nested.merge_result(place, column)
        }
    }

    fn need_manual_drop_state(&self) -> bool {
        self.nested.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        self.nested.drop_state(place)
    }

    fn convert_const_to_full(&self) -> bool {
        self.nested.convert_const_to_full()
    }

    fn get_if_condition(&self, columns: &[ColumnRef]) -> Option<Bitmap> {
        self.nested.get_if_condition(columns)
    }
}

impl<const NULLABLE_RESULT: bool> fmt::Display for AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AggregateNullUnaryAdaptor")
    }
}
