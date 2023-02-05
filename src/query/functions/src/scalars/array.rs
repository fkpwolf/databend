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

use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

use common_expression::types::array::ArrayColumnBuilder;
use common_expression::types::boolean::BooleanDomain;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::number::SimpleDomain;
use common_expression::types::number::UInt64Type;
use common_expression::types::ArgType;
use common_expression::types::ArrayType;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::DateType;
use common_expression::types::EmptyArrayType;
use common_expression::types::GenericType;
use common_expression::types::NullType;
use common_expression::types::NullableType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::types::ValueType;
use common_expression::types::ALL_NUMERICS_TYPES;
use common_expression::vectorize_1_arg;
use common_expression::vectorize_2_arg;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::vectorize_with_builder_3_arg;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Domain;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::ScalarRef;
use common_expression::Value;
use common_expression::ValueRef;
use common_hashtable::HashtableKeyable;
use common_hashtable::KeysRef;
use common_hashtable::StackHashSet;
use itertools::Itertools;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("contains", &["array_contains"]);
    registry.register_aliases("get", &["array_get"]);
    registry.register_aliases("length", &["array_length"]);
    registry.register_aliases("slice", &["array_slice"]);

    registry.register_0_arg_core::<EmptyArrayType, _, _>(
        "array",
        FunctionProperty::default(),
        || FunctionDomain::Full,
        |_| Value::Scalar(()),
    );

    registry.register_function_factory("array", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "array".to_string(),
                args_type: vec![DataType::Generic(0); args_type.len()],
                return_type: DataType::Array(Box::new(DataType::Generic(0))),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| {
                FunctionDomain::Domain(args_domain.iter().fold(Domain::Array(None), |acc, x| {
                    acc.merge(&Domain::Array(Some(Box::new(x.clone()))))
                }))
            }),
            eval: Box::new(|args, ctx| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });

                let mut builder: ArrayColumnBuilder<GenericType<0>> =
                    ArrayColumnBuilder::with_capacity(len.unwrap_or(1), 0, ctx.generics);

                for idx in 0..(len.unwrap_or(1)) {
                    for arg in args {
                        match arg {
                            ValueRef::Scalar(scalar) => {
                                builder.put_item(scalar.clone());
                            }
                            ValueRef::Column(col) => unsafe {
                                builder.put_item(col.index_unchecked(idx));
                            },
                        }
                    }
                    builder.commit_row();
                }

                match len {
                    Some(_) => Value::Column(Column::Array(Box::new(builder.build().upcast()))),
                    None => Value::Scalar(Scalar::Array(builder.build_scalar())),
                }
            }),
        }))
    });

    registry.register_1_arg::<EmptyArrayType, NumberType<u8>, _, _>(
        "length",
        FunctionProperty::default(),
        |_| FunctionDomain::Domain(SimpleDomain { min: 0, max: 0 }),
        |_, _| 0u8,
    );

    registry.register_1_arg::<ArrayType<GenericType<0>>, NumberType<u64>, _, _>(
        "length",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        |arr, _| arr.len() as u64,
    );

    registry.register_2_arg_core::<NullableType<EmptyArrayType>, NullableType<UInt64Type>, NullType, _, _>(
        "get",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar(()),
    );

    registry.register_combine_nullable_2_arg::<ArrayType<NullableType<GenericType<0>>>, UInt64Type, GenericType<0>, _, _>(
        "get",
        FunctionProperty::default(),
        |domain, _| FunctionDomain::Domain(NullableDomain {
            has_null: true,
            value: domain.as_ref().and_then(|domain| domain.value.clone()),
        }),
        vectorize_with_builder_2_arg::<ArrayType<NullableType<GenericType<0>>>, UInt64Type, NullableType<GenericType<0>>>(
            |arr, idx, output, _| {
                if idx == 0 {
                    output.push_null();
                } else {
                    match arr.index(idx as usize - 1) {
                        Some(Some(item)) => output.push(item),
                        _ => output.push_null(),
                    }
                }
            }
        ),
    );

    registry.register_combine_nullable_2_arg::<ArrayType<GenericType<0>>, UInt64Type, GenericType<0>, _, _>(
        "get",
        FunctionProperty::default(),
        |domain, _| FunctionDomain::Domain(NullableDomain {
            has_null: true,
            value: domain.as_ref().map(|domain| Box::new(domain.clone()))
        }),
        vectorize_with_builder_2_arg::<ArrayType<GenericType<0>>, UInt64Type, NullableType<GenericType<0>>>(
            |arr, idx, output, _| {
                if idx == 0 {
                    output.push_null();
                } else {
                    match arr.index(idx as usize - 1) {
                        Some(item) => output.push(item),
                        None => output.push_null(),
                    }
                }
            }
        ),
    );

    registry.register_2_arg_core::<NullType, NullType, NullType, _, _>(
        "array_indexof",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar(()),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<GenericType<0>>, GenericType<0>, UInt64Type, _, _>(
        "array_indexof",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<ArrayType<GenericType<0>>, GenericType<0>, UInt64Type>(
            |arr, val, output, _| {
                output.push(arr.iter().position(|item| item == val).map(|pos| pos+1).unwrap_or(0) as u64);
            },
        ),
    );

    registry.register_2_arg_core::<NullableType<EmptyArrayType>, NullableType<EmptyArrayType>, EmptyArrayType, _, _>(
        "array_concat",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar(()),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, _, _>(
        "array_concat",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>(
            |lhs, rhs, output, _| {
                output.builder.append_column(&lhs);
                output.builder.append_column(&rhs);
                output.commit_row()
            }
        ),
    );

    registry.register_passthrough_nullable_3_arg::<EmptyArrayType, UInt64Type, UInt64Type, EmptyArrayType, _, _>(
        "slice",
        FunctionProperty::default(),
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_3_arg::<EmptyArrayType, UInt64Type, UInt64Type, EmptyArrayType>(
            |_, _, _, output, _| {
                *output += 1;
            }
        ),
    );

    registry.register_passthrough_nullable_3_arg::<ArrayType<GenericType<0>>, UInt64Type, UInt64Type, ArrayType<GenericType<0>>, _, _>(
        "slice",
        FunctionProperty::default(),
        |domain,_, _| FunctionDomain::Domain(domain.clone()),
        vectorize_with_builder_3_arg::<ArrayType<GenericType<0>>, UInt64Type, UInt64Type, ArrayType<GenericType<0>>>(
            |arr, start, end, output, _| {
                let start = if start > 0 {
                    start as usize - 1
                } else {
                    start as usize
                };
                let end = end as usize;

                if arr.len() == 0 || start >= arr.len() || start >= end {
                    output.push_default();
                } else {
                    let range = if end < arr.len() {
                        Range { start, end }
                    } else {
                        Range { start, end: arr.len() }
                    };
                    let arr_slice = arr.slice(range);
                    output.push(arr_slice);
                }
            }
        ),
    );

    registry.register_passthrough_nullable_1_arg::<EmptyArrayType, EmptyArrayType, _, _>(
        "array_remove_first",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<EmptyArrayType, EmptyArrayType>(|_, output, _| {
            *output += 1;
        }),
    );

    registry.register_passthrough_nullable_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, _, _>(
        "array_remove_first",
        FunctionProperty::default(),
        |domain| FunctionDomain::Domain(domain.clone()),
        vectorize_with_builder_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>(
            |arr, output, _| {
                if arr.len() <= 1 {
                    output.push_default();
                } else {
                    let range = Range { start: 1, end: arr.len() };
                    let arr_slice = arr.slice(range);
                    output.push(arr_slice);
                }
            }
        ),
    );

    registry.register_passthrough_nullable_1_arg::<EmptyArrayType, EmptyArrayType, _, _>(
        "array_remove_last",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<EmptyArrayType, EmptyArrayType>(|_, output, _| {
            *output += 1;
        }),
    );

    registry.register_passthrough_nullable_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, _, _>(
        "array_remove_last",
        FunctionProperty::default(),
        |domain| FunctionDomain::Domain(domain.clone()),
        vectorize_with_builder_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>(
            |arr, output, _| {
                if arr.len() <= 1 {
                    output.push_default();
                } else {
                    let range = Range { start: 0, end: arr.len() - 1 };
                    let arr_slice = arr.slice(range);
                    output.push(arr_slice);
                }
            }
        ),
    );

    registry.register_2_arg_core::<GenericType<0>, ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, _, _>(
        "array_prepend",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        vectorize_2_arg::<GenericType<0>, ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>(|val, arr, _| {
            let data_type = arr.data_type();
            let mut builder = ColumnBuilder::with_capacity(&data_type, arr.len() + 1);
            builder.push(val);
            builder.append_column(&arr);
            builder.build()
        }),
    );

    registry.register_2_arg_core::<ArrayType<GenericType<0>>, GenericType<0>, ArrayType<GenericType<0>>, _, _>(
        "array_append",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        vectorize_2_arg::<ArrayType<GenericType<0>>, GenericType<0>, ArrayType<GenericType<0>>>(|arr, val, _| {
            let data_type = arr.data_type();
            let mut builder = ColumnBuilder::with_capacity(&data_type, arr.len() + 1);
            builder.append_column(&arr);
            builder.push(val);
            builder.build()
        }),
    );

    fn eval_contains<T: ArgType>(
        lhs: ValueRef<ArrayType<T>>,
        rhs: ValueRef<T>,
    ) -> Value<BooleanType>
    where
        T::Scalar: HashtableKeyable,
    {
        match lhs {
            ValueRef::Scalar(array) => {
                let mut set = StackHashSet::<_, 128>::with_capacity(T::column_len(&array));
                for val in T::iter_column(&array) {
                    let _ = set.set_insert(T::to_owned_scalar(val));
                }
                match rhs {
                    ValueRef::Scalar(c) => Value::Scalar(set.contains(&T::to_owned_scalar(c))),
                    ValueRef::Column(col) => {
                        let result = BooleanType::column_from_iter(
                            T::iter_column(&col).map(|c| set.contains(&T::to_owned_scalar(c))),
                            &[],
                        );
                        Value::Column(result)
                    }
                }
            }
            ValueRef::Column(array_column) => {
                let result = match rhs {
                    ValueRef::Scalar(c) => BooleanType::column_from_iter(
                        array_column.iter().map(|array| {
                            T::iter_column(&array).contains(&T::upcast_gat(c.clone()))
                        }),
                        &[],
                    ),
                    ValueRef::Column(col) => BooleanType::column_from_iter(
                        array_column
                            .iter()
                            .zip(T::iter_column(&col))
                            .map(|(array, c)| T::iter_column(&array).contains(&T::upcast_gat(c))),
                        &[],
                    ),
                };
                Value::Column(result)
            }
        }
    }

    for left in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match left {
            NumberDataType::NUM_TYPE => {
                registry.register_passthrough_nullable_2_arg::<ArrayType<NumberType<NUM_TYPE>>, NumberType<NUM_TYPE>, BooleanType, _, _>(
                    "contains",
                    FunctionProperty::default(),
                    |lhs, rhs| {
                        let has_true = match lhs {
                            Some(lhs) => !(lhs.min > rhs.max || lhs.max < rhs.min),
                            None => false,
                        };
                        FunctionDomain::Domain(BooleanDomain {
                            has_false: true,
                            has_true,
                        })
                    },
                    |lhs, rhs, _| eval_contains::<NumberType<NUM_TYPE>>(lhs, rhs)
                );
            }
        });
    }

    registry.register_passthrough_nullable_2_arg::<ArrayType<StringType>, StringType, BooleanType, _, _>(
        "contains",
        FunctionProperty::default(),
        |_, _| {
            FunctionDomain::Full
        },
        |lhs, rhs, _| {
            match lhs {
                ValueRef::Scalar(array) => {
                    let mut set = StackHashSet::<_, 128>::with_capacity(StringType::column_len(&array));
                    for val in array.iter() {
                        let key_ref = KeysRef::create(val.as_ptr() as usize, val.len());
                        let _ = set.set_insert(key_ref);
                    }
                    match rhs {
                        ValueRef::Scalar(val) =>  {
                            let key_ref = KeysRef::create(val.as_ptr() as usize, val.len());
                            Value::Scalar(set.contains( &key_ref))
                        },
                        ValueRef::Column(col) => {
                            let result = BooleanType::column_from_iter(StringType::iter_column(&col).map(|val| {
                                let key_ref = KeysRef::create(val.as_ptr() as usize, val.len());
                                set.contains(&key_ref)
                            }), &[]);
                            Value::Column(result)
                        }
                    }
                },
                ValueRef::Column(array_column) => {
                    let result = match rhs {
                        ValueRef::Scalar(c) =>  BooleanType::column_from_iter(array_column
                            .iter()
                            .map(|array| StringType::iter_column(&array).contains(&c)), &[]),
                        ValueRef::Column(col) =>  BooleanType::column_from_iter(array_column
                            .iter()
                            .zip(StringType::iter_column(&col))
                            .map(|(array, c)| StringType::iter_column(&array).contains(&c)), &[]),
                    };
                    Value::Column(result)
                }
            }
        }
    );

    registry
        .register_passthrough_nullable_2_arg::<ArrayType<DateType>, DateType, BooleanType, _, _>(
            "contains",
            FunctionProperty::default(),
            |lhs, rhs| {
                let has_true = match lhs {
                    Some(lhs) => !(lhs.min > rhs.max || lhs.max < rhs.min),
                    None => false,
                };
                FunctionDomain::Domain(BooleanDomain {
                    has_false: true,
                    has_true,
                })
            },
            |lhs, rhs, _| eval_contains::<DateType>(lhs, rhs),
        );

    registry.register_passthrough_nullable_2_arg::<ArrayType<TimestampType>, TimestampType, BooleanType, _, _>(
            "contains",
            FunctionProperty::default(),
            |lhs, rhs| {
                let has_true = match lhs {
                    Some(lhs) => !(lhs.min > rhs.max || lhs.max < rhs.min),
                    None => false,
                };
                FunctionDomain::Domain(BooleanDomain {
                    has_false: true,
                    has_true,
                })
            },
            |lhs, rhs, _| eval_contains::<TimestampType>(lhs, rhs)
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<BooleanType>, BooleanType, BooleanType, _, _>(
        "contains",
        FunctionProperty::default(),
        |lhs, rhs| {
            match lhs {
                Some(lhs) => {
                    FunctionDomain::Domain(BooleanDomain {
                        has_false:  (lhs.has_false && rhs.has_true) || (lhs.has_true && rhs.has_false),
                        has_true:   (lhs.has_false && rhs.has_false) || (lhs.has_true && rhs.has_true),
                    })
                },
                None => FunctionDomain::Domain(BooleanDomain {
                    has_false: false,
                    has_true: false,
                }),
            }
        },
        |lhs, rhs, _| {
            match lhs {
                ValueRef::Scalar(array) => {
                    let mut set = StackHashSet::<_, 128>::with_capacity(BooleanType::column_len(&array));
                    for val in array.iter() {
                        let _ = set.set_insert(val as u8);
                    }
                    match rhs {
                        ValueRef::Scalar(val) =>  {
                            Value::Scalar(set.contains( &(val as u8)))
                        },
                        ValueRef::Column(col) => {
                            let result = BooleanType::column_from_iter(BooleanType::iter_column(&col).map(|val| {
                                set.contains(&(val as u8))
                            }), &[]);
                            Value::Column(result)
                        }
                    }
                },
                ValueRef::Column(array_column) => {
                    let result = match rhs {
                        ValueRef::Scalar(c) =>  BooleanType::column_from_iter(array_column
                            .iter()
                            .map(|array| BooleanType::iter_column(&array).contains(&c)), &[]),
                        ValueRef::Column(col) =>  BooleanType::column_from_iter(array_column
                            .iter()
                            .zip(BooleanType::iter_column(&col))
                            .map(|(array, c)| BooleanType::iter_column(&array).contains(&c)), &[]),
                    };
                    Value::Column(result)
                }
            }
        }
    );

    registry.register_2_arg_core::<ArrayType<GenericType<0>>, GenericType<0>, BooleanType, _, _>(
        "contains",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        vectorize_2_arg::<ArrayType<GenericType<0>>, GenericType<0>, BooleanType>(|lhs, rhs, _| {
            lhs.iter().contains(&rhs)
        }),
    );

    registry.register_passthrough_nullable_1_arg::<EmptyArrayType, UInt64Type, _, _>(
        "array_unique",
        FunctionProperty::default(),
        |_| FunctionDomain::Domain(SimpleDomain { min: 0, max: 0 }),
        vectorize_1_arg::<EmptyArrayType, UInt64Type>(|_, _| 0),
    );

    registry.register_passthrough_nullable_1_arg::<ArrayType<GenericType<0>>, UInt64Type, _, _>(
        "array_unique",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_1_arg::<ArrayType<GenericType<0>>, UInt64Type>(|arr, _| {
            if arr.len() > 0 {
                let mut set: StackHashSet<u128, 16> = StackHashSet::with_capacity(arr.len());
                for val in arr.iter() {
                    if val == ScalarRef::Null {
                        continue;
                    }
                    let mut hasher = SipHasher24::new();
                    val.hash(&mut hasher);
                    let hash128 = hasher.finish128();
                    let _ = set.set_insert(hash128.into());
                }
                set.len() as u64
            } else {
                0
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<EmptyArrayType, EmptyArrayType, _, _>(
        "array_distinct",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_1_arg::<EmptyArrayType, EmptyArrayType>(|arr, _| arr),
    );

    registry.register_passthrough_nullable_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, _, _>(
        "array_distinct",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>(|arr, _| {
            if arr.len() > 0 {
                let data_type = arr.data_type();
                let mut builder = ColumnBuilder::with_capacity(&data_type, arr.len());
                let mut set: StackHashSet<u128, 16> = StackHashSet::with_capacity(arr.len());
                for val in arr.iter() {
                    if val == ScalarRef::Null {
                        continue;
                    }
                    let mut hasher = SipHasher24::new();
                    val.hash(&mut hasher);
                    let hash128 = hasher.finish128();
                    let key = hash128.into();
                    if !set.contains(&key) {
                        let _ = set.set_insert(key);
                        builder.push(val);
                    }
                }
                builder.build()
            } else {
                arr
            }
        }),
    );
}
