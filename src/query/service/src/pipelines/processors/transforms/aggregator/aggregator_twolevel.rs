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

use std::time::Instant;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::HashMethod;
use common_expression::Value;
use common_functions::aggregates::StateAddr;
use common_hashtable::FastHash;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use tracing::info;

use crate::pipelines::processors::transforms::aggregator::aggregate_info::AggregateInfo;
use crate::pipelines::processors::transforms::aggregator::aggregator_final_parallel::ParallelFinalAggregator;
use crate::pipelines::processors::transforms::aggregator::PartialAggregator;
use crate::pipelines::processors::transforms::aggregator::SingleStateAggregator;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::group_by::TwoLevelHashMethod;
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::processors::AggregatorParams;

pub trait TwoLevelAggregatorLike
where Self: Aggregator + Send
{
    const SUPPORT_TWO_LEVEL: bool;

    type TwoLevelAggregator: Aggregator;

    fn get_state_cardinality(&self) -> usize {
        0
    }

    fn convert_two_level(self) -> Result<TwoLevelAggregator<Self>> {
        Err(ErrorCode::Unimplemented(format!(
            "Two level aggregator is unimplemented for {}",
            Self::NAME
        )))
    }

    fn convert_two_level_block(_agg: &mut Self::TwoLevelAggregator) -> Result<Vec<DataBlock>> {
        Err(ErrorCode::Unimplemented(format!(
            "Two level aggregator is unimplemented for {}",
            Self::NAME
        )))
    }
}

impl<Method> TwoLevelAggregatorLike for PartialAggregator<true, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = Method::SUPPORT_TWO_LEVEL;

    type TwoLevelAggregator = PartialAggregator<true, TwoLevelHashMethod<Method>>;

    fn get_state_cardinality(&self) -> usize {
        self.hash_table.len()
    }

    // PartialAggregator<true, Method> -> TwoLevelAggregator<PartialAggregator<true, Method>>
    fn convert_two_level(mut self) -> Result<TwoLevelAggregator<Self>> {
        let instant = Instant::now();
        let method = self.method.clone();
        let two_level_method = TwoLevelHashMethod::create(method);
        let mut two_level_hashtable = two_level_method.create_hash_table()?;

        unsafe {
            for item in self.hash_table.iter() {
                match two_level_hashtable.insert_and_entry(item.key()) {
                    Ok(mut entry) => {
                        *entry.get_mut() = *item.get();
                    }
                    Err(mut entry) => {
                        *entry.get_mut() = *item.get();
                    }
                };
            }
        }

        info!(
            "Convert to two level aggregator elapsed: {:?}",
            instant.elapsed()
        );

        self.states_dropped = true;
        Ok(TwoLevelAggregator::<Self> {
            inner: PartialAggregator::<true, TwoLevelHashMethod<Method>> {
                area: self.area.take(),
                params: self.params.clone(),
                states_dropped: false,
                method: two_level_method,
                hash_table: two_level_hashtable,
            },
        })
    }

    fn convert_two_level_block(agg: &mut Self::TwoLevelAggregator) -> Result<Vec<DataBlock>> {
        let mut data_blocks = Vec::with_capacity(256);

        fn clear_table<T: HashtableLike<Value = usize>>(table: &mut T, params: &AggregatorParams) {
            let aggregate_functions = &params.aggregate_functions;
            let offsets_aggregate_states = &params.offsets_aggregate_states;

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

            for group_entity in table.iter() {
                let place = Into::<StateAddr>::into(*group_entity.get());

                for (function, state_offset) in functions.iter().zip(states.iter()) {
                    unsafe { function.drop_state(place.next(*state_offset)) }
                }
            }

            table.clear();
        }

        for (bucket, inner_table) in agg.hash_table.iter_tables_mut().enumerate() {
            if inner_table.len() == 0 {
                continue;
            }

            let iterator = inner_table.iter();

            let (capacity, _) = iterator.size_hint();

            let aggregator_params = agg.params.as_ref();
            let funcs = &aggregator_params.aggregate_functions;
            let aggr_len = funcs.len();
            let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

            // Builders.
            let mut state_builders: Vec<StringColumnBuilder> = (0..aggr_len)
                .map(|_| StringColumnBuilder::with_capacity(capacity, capacity * 4))
                .collect();

            let mut group_key_builder = agg.method.keys_column_builder(capacity);

            for group_entity in iterator {
                let place = Into::<StateAddr>::into(*group_entity.get());

                for (idx, func) in funcs.iter().enumerate() {
                    let arg_place = place.next(offsets_aggregate_states[idx]);
                    func.serialize(arg_place, &mut state_builders[idx].data)?;
                    state_builders[idx].commit_row();
                }

                group_key_builder.append_value(group_entity.key());
            }

            let mut columns = Vec::with_capacity(state_builders.len() + 1);
            let mut num_rows = 0;
            for builder in state_builders.into_iter() {
                let col = builder.build();
                num_rows = col.len();
                columns.push(BlockEntry {
                    value: Value::Column(Column::String(col)),
                    data_type: DataType::String,
                });
            }

            let col = group_key_builder.finish();
            let group_key_type = col.data_type();

            columns.push(BlockEntry {
                value: Value::Column(col),
                data_type: group_key_type,
            });

            data_blocks.push(DataBlock::new_with_meta(
                columns,
                num_rows,
                Some(AggregateInfo::create(bucket as isize)),
            ));

            clear_table(inner_table, &agg.params);
        }

        drop(agg.area.take());
        agg.states_dropped = true;
        Ok(data_blocks)
    }
}

impl<Method> TwoLevelAggregatorLike for PartialAggregator<false, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = Method::SUPPORT_TWO_LEVEL;

    type TwoLevelAggregator = PartialAggregator<false, TwoLevelHashMethod<Method>>;

    fn get_state_cardinality(&self) -> usize {
        self.hash_table.len()
    }

    // PartialAggregator<false, Method> -> TwoLevelAggregator<PartialAggregator<false, Method>>
    fn convert_two_level(mut self) -> Result<TwoLevelAggregator<Self>> {
        let instant = Instant::now();
        let method = self.method.clone();
        let two_level_method = TwoLevelHashMethod::create(method);
        let mut two_level_hashtable = two_level_method.create_hash_table()?;

        unsafe {
            for item in self.hash_table.iter() {
                match two_level_hashtable.insert_and_entry(item.key()) {
                    Ok(mut entry) => {
                        *entry.get_mut() = *item.get();
                    }
                    Err(mut entry) => {
                        *entry.get_mut() = *item.get();
                    }
                };
            }
        }

        info!(
            "Convert to two level aggregator elapsed: {:?}",
            instant.elapsed()
        );

        self.states_dropped = true;
        Ok(TwoLevelAggregator::<Self> {
            inner: PartialAggregator::<false, TwoLevelHashMethod<Method>> {
                area: self.area.take(),
                params: self.params.clone(),
                states_dropped: false,
                method: two_level_method,
                hash_table: two_level_hashtable,
            },
        })
    }

    fn convert_two_level_block(agg: &mut Self::TwoLevelAggregator) -> Result<Vec<DataBlock>> {
        let mut chunks = Vec::with_capacity(256);
        for (bucket, inner_table) in agg.hash_table.iter_tables_mut().enumerate() {
            if inner_table.len() == 0 {
                continue;
            }

            let iterator = inner_table.iter();

            let (capacity, _) = iterator.size_hint();
            let mut keys_column_builder = agg.method.keys_column_builder(capacity);

            for group_entity in iterator {
                keys_column_builder.append_value(group_entity.key());
            }

            let column = keys_column_builder.finish();
            let num_rows = column.len();
            let column = BlockEntry {
                data_type: column.data_type(),
                value: Value::Column(column),
            };

            chunks.push(DataBlock::new_with_meta(
                vec![column],
                num_rows,
                Some(AggregateInfo::create(bucket as isize)),
            ));

            inner_table.clear();
        }

        drop(agg.area.take());
        agg.states_dropped = true;
        Ok(chunks)
    }
}

impl TwoLevelAggregatorLike for SingleStateAggregator<true> {
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = SingleStateAggregator<true>;
}

impl TwoLevelAggregatorLike for SingleStateAggregator<false> {
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = SingleStateAggregator<false>;
}

impl<Method> TwoLevelAggregatorLike for ParallelFinalAggregator<true, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = ParallelFinalAggregator<true, TwoLevelHashMethod<Method>>;
}

impl<Method> TwoLevelAggregatorLike for ParallelFinalAggregator<false, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = ParallelFinalAggregator<false, TwoLevelHashMethod<Method>>;
}

// Example: TwoLevelAggregator<PartialAggregator<true, Method>> ->
//      TwoLevelAggregator {
//          inner: PartialAggregator<true, TwoLevelMethod<Method>>
//      }
pub struct TwoLevelAggregator<T: TwoLevelAggregatorLike> {
    inner: T::TwoLevelAggregator,
}

impl<T: TwoLevelAggregatorLike> Aggregator for TwoLevelAggregator<T> {
    const NAME: &'static str = "TwoLevelAggregator";

    #[inline(always)]
    fn consume(&mut self, data: DataBlock) -> Result<()> {
        self.inner.consume(data)
    }

    #[inline(always)]
    fn generate(&mut self) -> Result<Vec<DataBlock>> {
        T::convert_two_level_block(&mut self.inner)
    }
}
