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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodKeysU128;
use common_datablocks::HashMethodKeysU16;
use common_datablocks::HashMethodKeysU256;
use common_datablocks::HashMethodKeysU32;
use common_datablocks::HashMethodKeysU512;
use common_datablocks::HashMethodKeysU64;
use common_datablocks::HashMethodKeysU8;
use common_datablocks::HashMethodSerializer;
use common_datavalues::ColumnRef;
use common_datavalues::MutableColumn;
use common_datavalues::MutableStringColumn;
use common_datavalues::ScalarColumnBuilder;
use common_exception::Result;
use common_functions::aggregates::StateAddr;
use common_functions::aggregates::StateAddrs;

use crate::pipelines::processors::transforms::group_by::AggregatorState;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::group_by::StateEntityMutRef;
use crate::pipelines::processors::transforms::group_by::StateEntityRef;
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::processors::AggregatorParams;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub type KeysU8PartialAggregator<const HAS_AGG: bool> =
    PartialAggregator<HAS_AGG, HashMethodKeysU8>;
pub type KeysU16PartialAggregator<const HAS_AGG: bool> =
    PartialAggregator<HAS_AGG, HashMethodKeysU16>;
pub type KeysU32PartialAggregator<const HAS_AGG: bool> =
    PartialAggregator<HAS_AGG, HashMethodKeysU32>;
pub type KeysU64PartialAggregator<const HAS_AGG: bool> =
    PartialAggregator<HAS_AGG, HashMethodKeysU64>;
pub type KeysU128PartialAggregator<const HAS_AGG: bool> =
    PartialAggregator<HAS_AGG, HashMethodKeysU128>;

pub type KeysU256PartialAggregator<const HAS_AGG: bool> =
    PartialAggregator<HAS_AGG, HashMethodKeysU256>;

pub type KeysU512PartialAggregator<const HAS_AGG: bool> =
    PartialAggregator<HAS_AGG, HashMethodKeysU512>;

pub type SerializerPartialAggregator<const HAS_AGG: bool> =
    PartialAggregator<HAS_AGG, HashMethodSerializer>;

pub struct PartialAggregator<
    const HAS_AGG: bool,
    Method: HashMethod + PolymorphicKeysHelper<Method>,
> {
    is_generated: bool,
    states_dropped: bool,

    method: Method,
    state: Method::State,
    params: Arc<AggregatorParams>,
    ctx: Arc<QueryContext>,
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method> + Send>
    PartialAggregator<HAS_AGG, Method>
{
    pub fn create(ctx: Arc<QueryContext>, method: Method, params: Arc<AggregatorParams>) -> Self {
        let state = method.aggregate_state();
        Self {
            is_generated: false,
            states_dropped: false,
            state,
            method,
            params,
            ctx,
        }
    }

    #[inline(always)]
    fn lookup_key(keys_iter: Method::HashKeyIter<'_>, state: &mut Method::State) {
        let mut inserted = true;
        for key in keys_iter {
            state.entity(key, &mut inserted);
        }
    }

    /// Allocate aggregation function state for each key(the same key can always get the same state)
    #[inline(always)]
    fn lookup_state(
        params: &Arc<AggregatorParams>,
        keys_iter: Method::HashKeyIter<'_>,
        state: &mut Method::State,
    ) -> StateAddrs {
        let mut places = Vec::with_capacity(keys_iter.size_hint().0);

        let mut inserted = true;
        for key in keys_iter {
            let unsafe_state = state as *mut Method::State;
            let mut entity = state.entity(key, &mut inserted);

            match inserted {
                true => {
                    if let Some(place) = unsafe { (*unsafe_state).alloc_layout(params) } {
                        places.push(place);
                        entity.set_state_value(place.addr());
                    }
                }
                false => {
                    let place: StateAddr = entity.get_state_value().into();
                    places.push(place);
                }
            }
        }
        places
    }

    #[inline(always)]
    fn aggregate_arguments(
        block: &DataBlock,
        params: &Arc<AggregatorParams>,
    ) -> Result<Vec<Vec<ColumnRef>>> {
        let aggregate_functions_arguments = &params.aggregate_functions_arguments;
        let mut aggregate_arguments_columns =
            Vec::with_capacity(aggregate_functions_arguments.len());
        for function_arguments in aggregate_functions_arguments {
            let mut function_arguments_column = Vec::with_capacity(function_arguments.len());

            for argument_index in function_arguments {
                let argument_column = block.column(*argument_index);
                function_arguments_column.push(argument_column.clone());
            }

            aggregate_arguments_columns.push(function_arguments_column);
        }

        Ok(aggregate_arguments_columns)
    }

    #[inline(always)]
    #[allow(clippy::ptr_arg)] // &[StateAddr] slower than &StateAddrs ~20%
    fn execute(
        params: &Arc<AggregatorParams>,
        block: &DataBlock,
        places: &StateAddrs,
    ) -> Result<()> {
        let aggregate_functions = &params.aggregate_functions;
        let offsets_aggregate_states = &params.offsets_aggregate_states;
        let aggregate_arguments_columns = Self::aggregate_arguments(block, params)?;

        // This can benificial for the case of dereferencing
        // This will help improve the performance ~hundreds of megabits per second
        let aggr_arg_columns_slice = &aggregate_arguments_columns;

        for index in 0..aggregate_functions.len() {
            let rows = block.num_rows();
            let function = &aggregate_functions[index];
            let state_offset = offsets_aggregate_states[index];
            let function_arguments = &aggr_arg_columns_slice[index];
            function.accumulate_keys(places, state_offset, function_arguments, rows)?;
        }

        Ok(())
    }

    #[inline(always)]
    pub fn group_columns<'a>(indices: &[usize], block: &'a DataBlock) -> Vec<&'a ColumnRef> {
        indices
            .iter()
            .map(|&index| block.column(index))
            .collect::<Vec<&ColumnRef>>()
    }

    #[inline(always)]
    fn generate_data(&mut self) -> Result<Option<DataBlock>> {
        if self.state.len() == 0 || self.is_generated {
            return Ok(None);
        }

        self.is_generated = true;
        let state_groups_len = self.state.len();
        let aggregator_params = self.params.as_ref();
        let funcs = &aggregator_params.aggregate_functions;
        let aggr_len = funcs.len();
        let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

        // Builders.
        let mut state_builders: Vec<MutableStringColumn> = (0..aggr_len)
            .map(|_| MutableStringColumn::with_capacity(state_groups_len * 4))
            .collect();

        let mut group_key_builder = self.method.keys_column_builder(state_groups_len);
        for group_entity in self.state.iter() {
            let place: StateAddr = group_entity.get_state_value().into();

            for (idx, func) in funcs.iter().enumerate() {
                let arg_place = place.next(offsets_aggregate_states[idx]);
                func.serialize(arg_place, state_builders[idx].values_mut())?;
                state_builders[idx].commit_row();
            }

            group_key_builder.append_value(group_entity.get_state_key());
        }

        let schema = &self.params.output_schema;
        let mut columns: Vec<ColumnRef> = Vec::with_capacity(schema.fields().len());
        for mut builder in state_builders {
            columns.push(builder.to_column());
        }

        columns.push(group_key_builder.finish());
        Ok(Some(DataBlock::create(schema.clone(), columns)))
    }
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator
    for PartialAggregator<true, Method>
{
    const NAME: &'static str = "GroupByPartialTransform";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        // 1.1 and 1.2.
        let group_columns = Self::group_columns(&self.params.group_columns, &block);
        let group_keys_state = self
            .method
            .build_keys_state(&group_columns, block.num_rows())?;

        let group_keys_iter = self.method.build_keys_iter(&group_keys_state)?;

        let group_by_two_level_threshold =
            self.ctx.get_settings().get_group_by_two_level_threshold()? as usize;
        if !self.state.is_two_level() && self.state.len() >= group_by_two_level_threshold {
            self.state.convert_to_twolevel();
        }

        let places = Self::lookup_state(&self.params, group_keys_iter, &mut self.state);
        Self::execute(&self.params, &block, &places)
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        self.generate_data()
    }
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator
    for PartialAggregator<false, Method>
{
    const NAME: &'static str = "GroupByPartialTransform";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        // 1.1 and 1.2.
        let group_columns = Self::group_columns(&self.params.group_columns, &block);

        let keys_state = self
            .method
            .build_keys_state(&group_columns, block.num_rows())?;
        let group_keys_iter = self.method.build_keys_iter(&keys_state)?;

        let group_by_two_level_threshold =
            self.ctx.get_settings().get_group_by_two_level_threshold()? as usize;
        if !self.state.is_two_level() && self.state.len() >= group_by_two_level_threshold {
            self.state.convert_to_twolevel();
        }

        Self::lookup_key(group_keys_iter, &mut self.state);
        Ok(())
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.state.len() == 0 || self.is_generated {
            true => {
                self.drop_states();
                Ok(None)
            }
            false => {
                self.is_generated = true;
                let mut keys_column_builder = self.method.keys_column_builder(self.state.len());
                for group_entity in self.state.iter() {
                    keys_column_builder.append_value(group_entity.get_state_key());
                }

                let columns = keys_column_builder.finish();
                Ok(Some(DataBlock::create(
                    self.params.output_schema.clone(),
                    vec![columns],
                )))
            }
        }
    }
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method>>
    PartialAggregator<HAS_AGG, Method>
{
    fn drop_states(&mut self) {
        if !self.states_dropped {
            let aggregator_params = self.params.as_ref();
            let aggregate_functions = &aggregator_params.aggregate_functions;
            let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

            let functions = aggregate_functions
                .iter()
                .filter(|p| p.need_manual_drop_state())
                .collect::<Vec<_>>();

            let states = offsets_aggregate_states
                .iter()
                .enumerate()
                .filter(|(idx, _)| aggregate_functions[*idx].need_manual_drop_state())
                .map(|(_, s)| *s)
                .collect::<Vec<_>>();

            for group_entity in self.state.iter() {
                let place: StateAddr = group_entity.get_state_value().into();

                for (function, state_offset) in functions.iter().zip(states.iter()) {
                    unsafe { function.drop_state(place.next(*state_offset)) }
                }
            }
            self.states_dropped = true;
        }
    }
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method>> Drop
    for PartialAggregator<HAS_AGG, Method>
{
    fn drop(&mut self) {
        self.drop_states();
    }
}
