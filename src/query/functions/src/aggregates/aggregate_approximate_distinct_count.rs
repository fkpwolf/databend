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
use common_io::prelude::*;
use streaming_algorithms::HyperLogLog;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;

/// Use Hyperloglog to estimate distinct of values
#[derive(serde::Serialize, serde::Deserialize)]
pub struct AggregateApproximateDistinctCountState {
    hll: HyperLogLog<DataValue>,
}

/// S: ScalarType
#[derive(Clone)]
pub struct AggregateApproximateDistinctCountFunction {
    display_name: String,
}

impl AggregateApproximateDistinctCountFunction {
    pub fn try_create(
        display_name: &str,
        _params: Vec<DataValue>,
        arguments: Vec<DataField>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        assert_unary_arguments(display_name, arguments.len())?;
        Ok(Arc::new(AggregateApproximateDistinctCountFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> AggregateFunctionDescription {
        let features = super::aggregate_function_factory::AggregateFunctionFeatures {
            returns_default_when_only_null: true,
            ..Default::default()
        };
        AggregateFunctionDescription::creator_with_features(Box::new(Self::try_create), features)
    }
}

impl AggregateFunction for AggregateApproximateDistinctCountFunction {
    fn name(&self) -> &str {
        "AggregateApproximateDistinctCountFunction"
    }

    fn return_type(&self) -> Result<DataTypeImpl> {
        Ok(u64::to_data_type())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateApproximateDistinctCountState {
            hll: HyperLogLog::new(0.04),
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateApproximateDistinctCountState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateApproximateDistinctCountState>();
        let column = &columns[0];

        let (_, bm) = column.validity();
        let bitmap = combine_validities(bm, validity);
        let nulls = match bitmap {
            Some(ref b) => b.unset_bits(),
            None => 0,
        };
        if column.len() == nulls {
            return Ok(());
        }

        if let Some(bitmap) = bitmap {
            for (i, value) in column.to_values().iter().enumerate() {
                if bitmap.get_bit(i) {
                    state.hll.push(value);
                }
            }
        } else {
            column.to_values().iter().for_each(|value| {
                state.hll.push(value);
            });
        }

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], _row: usize) -> Result<()> {
        let state = place.get::<AggregateApproximateDistinctCountState>();
        for column in columns {
            column.to_values().iter().for_each(|value| {
                state.hll.push(value);
            });
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<AggregateApproximateDistinctCountState>();
        serialize_into_buf(writer, state)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateApproximateDistinctCountState>();
        *state = deserialize_from_slice(reader)?;
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateApproximateDistinctCountState>();
        let rhs = rhs.get::<AggregateApproximateDistinctCountState>();
        state.hll.union(&rhs.hll);

        Ok(())
    }

    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        let builder: &mut MutablePrimitiveColumn<u64> = Series::check_get_mutable_column(array)?;
        let state = place.get::<AggregateApproximateDistinctCountState>();
        builder.append_value(state.hll.len() as u64);

        Ok(())
    }
}

impl fmt::Display for AggregateApproximateDistinctCountFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
