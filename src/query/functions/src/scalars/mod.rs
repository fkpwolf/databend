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

use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::ALL_INTEGER_TYPES;
use common_expression::FunctionRegistry;
use ctor::ctor;

mod arithmetic;
mod arithmetic_modulo;
mod array;
mod boolean;
mod control;
mod datetime;
mod geo;
mod math;
mod tuple;
mod variant;

mod comparison;
mod hash;
mod other;
mod string;
mod string_multi_args;
mod uuid;

pub use comparison::check_pattern_type;
pub use comparison::is_like_pattern_escape;
pub use comparison::PatternType;

use self::comparison::ALL_COMP_FUNC_NAMES;
use self::comparison::ALL_MATCH_FUNC_NAMES;

#[ctor]
pub static BUILTIN_FUNCTIONS: FunctionRegistry = builtin_functions();

fn builtin_functions() -> FunctionRegistry {
    let mut registry = FunctionRegistry::new();

    register_casting_rules(&mut registry);

    arithmetic::register(&mut registry);
    array::register(&mut registry);
    boolean::register(&mut registry);
    control::register(&mut registry);
    comparison::register(&mut registry);
    datetime::register(&mut registry);
    math::register(&mut registry);
    string::register(&mut registry);
    string_multi_args::register(&mut registry);
    tuple::register(&mut registry);
    variant::register(&mut registry);
    geo::register(&mut registry);
    hash::register(&mut registry);
    other::register(&mut registry);
    uuid::register(&mut registry);

    registry
}

fn register_casting_rules(registry: &mut FunctionRegistry) {
    for func_name in ["and", "or", "not", "xor"] {
        for data_type in ALL_INTEGER_TYPES {
            registry.register_auto_cast_signatures(func_name, vec![(
                DataType::Number(*data_type),
                DataType::Boolean,
            )]);
        }
    }

    for name in ALL_COMP_FUNC_NAMES {
        registry.register_auto_cast_signatures(name, vec![
            (DataType::String, DataType::Timestamp),
            (DataType::String, DataType::Date),
            (DataType::Date, DataType::Timestamp),
            (DataType::Boolean, DataType::Variant),
            (DataType::Date, DataType::Variant),
            (DataType::Timestamp, DataType::Variant),
            (DataType::String, DataType::Variant),
            (DataType::Number(NumberDataType::UInt8), DataType::Variant),
            (DataType::Number(NumberDataType::UInt16), DataType::Variant),
            (DataType::Number(NumberDataType::UInt32), DataType::Variant),
            (DataType::Number(NumberDataType::UInt64), DataType::Variant),
            (DataType::Number(NumberDataType::Int8), DataType::Variant),
            (DataType::Number(NumberDataType::Int16), DataType::Variant),
            (DataType::Number(NumberDataType::Int32), DataType::Variant),
            (DataType::Number(NumberDataType::Int64), DataType::Variant),
            (DataType::Number(NumberDataType::Float32), DataType::Variant),
            (DataType::Number(NumberDataType::Float64), DataType::Variant),
        ]);
    }

    registry.register_auto_cast_signatures("eq", vec![
        (DataType::String, DataType::Number(NumberDataType::Float64)),
        (DataType::String, DataType::Number(NumberDataType::Int64)),
    ]);

    for name in ALL_MATCH_FUNC_NAMES {
        registry.register_auto_cast_signatures(name, vec![(DataType::Variant, DataType::String)]);
    }
}
