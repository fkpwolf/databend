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
//

use std::sync::Arc;

use common_exception::Result;
use common_expression::type_check::check_function;
use common_expression::types::number::NumberScalar;
use common_expression::types::number::UInt8Type;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::Expr;
use common_expression::FromData;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_storages_index::BlockFilter;
use common_storages_index::FilterEvalResult;

#[test]
fn test_bloom_filter() -> Result<()> {
    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("0", TableDataType::Number(NumberDataType::UInt8)),
        TableField::new("1", TableDataType::String),
    ]));
    let chunks = vec![
        DataBlock::new(
            vec![
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::UInt8),
                    value: Value::Scalar(Scalar::Number(NumberScalar::UInt8(1))),
                },
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(b"a".to_vec())),
                },
            ],
            2,
        ),
        DataBlock::new_from_columns(vec![
            UInt8Type::from_data(vec![2, 3]),
            StringType::from_data(vec!["b", "c"]),
        ]),
    ];
    let chunks_ref = chunks.iter().collect::<Vec<_>>();

    let index = BlockFilter::try_create(FunctionContext::default(), schema, &chunks_ref)?;

    assert_eq!(
        FilterEvalResult::MustFalse,
        eval_index(
            &index,
            "0",
            Scalar::Number(NumberScalar::UInt8(0)),
            DataType::Number(NumberDataType::UInt8)
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(
            &index,
            "0",
            Scalar::Number(NumberScalar::UInt8(1)),
            DataType::Number(NumberDataType::UInt8)
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(
            &index,
            "0",
            Scalar::Number(NumberScalar::UInt8(2)),
            DataType::Number(NumberDataType::UInt8)
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(&index, "1", Scalar::String(b"a".to_vec()), DataType::String)
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(&index, "1", Scalar::String(b"b".to_vec()), DataType::String)
    );
    assert_eq!(
        FilterEvalResult::MustFalse,
        eval_index(&index, "1", Scalar::String(b"d".to_vec()), DataType::String)
    );

    Ok(())
}

fn eval_index(index: &BlockFilter, col_name: &str, val: Scalar, ty: DataType) -> FilterEvalResult {
    index
        .eval(
            check_function(
                None,
                "eq",
                &[],
                &[
                    Expr::ColumnRef {
                        span: None,
                        id: col_name.to_string(),
                        data_type: ty.clone(),
                    },
                    Expr::Constant {
                        span: None,
                        scalar: val,
                        data_type: ty,
                    },
                ],
                &BUILTIN_FUNCTIONS,
            )
            .unwrap(),
        )
        .unwrap()
}
