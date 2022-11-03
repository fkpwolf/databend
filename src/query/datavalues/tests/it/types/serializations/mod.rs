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

use common_datavalues::prelude::*;
use common_datavalues::serializations::NullSerializer;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use pretty_assertions::assert_eq;
use serde_json::json;

mod helpers;

#[test]
fn test_serializers() -> Result<()> {
    struct Test {
        name: &'static str,
        data_type: DataTypeImpl,
        column: ColumnRef,
        val_str: &'static str,
        col_str: Vec<String>,
    }

    let tests = vec![
        Test {
            name: "boolean",
            data_type: BooleanType::new_impl(),
            column: Series::from_data(vec![true, false, true]),
            val_str: "1",
            col_str: vec!["1".to_owned(), "0".to_owned(), "1".to_owned()],
        },
        Test {
            name: "int8",
            data_type: Int8Type::new_impl(),
            column: Series::from_data(vec![1i8, 2i8, 1]),
            val_str: "1",
            col_str: vec!["1".to_owned(), "2".to_owned(), "1".to_owned()],
        },
        Test {
            name: "datetime",
            data_type: TimestampType::new_impl(),
            column: Series::from_data(vec![1630320462000000i64, 1637117572000000i64, 1000000]),
            val_str: "2021-08-30 10:47:42.000000",
            col_str: vec![
                "2021-08-30 10:47:42.000000".to_owned(),
                "2021-11-17 02:52:52.000000".to_owned(),
                "1970-01-01 00:00:01.000000".to_owned(),
            ],
        },
        Test {
            name: "date32",
            data_type: DateType::new_impl(),
            column: Series::from_data(vec![18869i32, 18948i32, 1]),
            val_str: "2021-08-30",
            col_str: vec![
                "2021-08-30".to_owned(),
                "2021-11-17".to_owned(),
                "1970-01-02".to_owned(),
            ],
        },
        Test {
            name: "string",
            data_type: StringType::new_impl(),
            column: Series::from_data(vec!["hello", "world", "NULL"]),
            val_str: "hello",
            col_str: vec!["hello".to_owned(), "world".to_owned(), "NULL".to_owned()],
        },
        Test {
            name: "array",
            data_type: DataTypeImpl::Array(ArrayType::create(StringType::new_impl())),
            column: Arc::new(ArrayColumn::from_data(
                DataTypeImpl::Array(ArrayType::create(StringType::new_impl())),
                vec![0, 1, 3, 6].into(),
                Series::from_data(vec!["test", "data", "bend", "hello", "world", "NULL"]),
            )),
            val_str: "['test']",
            col_str: vec![
                "['test']".to_owned(),
                "['data', 'bend']".to_owned(),
                "['hello', 'world', 'NULL']".to_owned(),
            ],
        },
        Test {
            name: "struct",
            data_type: DataTypeImpl::Struct(StructType::create(
                Some(vec!["date".to_owned(), "integer".to_owned()]),
                vec![DateType::new_impl(), Int8Type::new_impl()],
            )),
            column: Arc::new(StructColumn::from_data(
                vec![
                    Series::from_data(vec![18869i32, 18948i32, 1]),
                    Series::from_data(vec![1i8, 2i8, 3]),
                ],
                DataTypeImpl::Struct(StructType::create(
                    Some(vec!["date".to_owned(), "integer".to_owned()]),
                    vec![DateType::new_impl(), Int8Type::new_impl()],
                )),
            )),
            val_str: "('2021-08-30', 1)",
            col_str: vec![
                "('2021-08-30', 1)".to_owned(),
                "('2021-11-17', 2)".to_owned(),
                "('1970-01-02', 3)".to_owned(),
            ],
        },
        Test {
            name: "variant",
            data_type: VariantType::new_impl(),
            column: Arc::new(VariantColumn::new_from_vec(vec![
                VariantValue::from(json!(null)),
                VariantValue::from(json!(true)),
                VariantValue::from(json!(false)),
                VariantValue::from(json!(123)),
                VariantValue::from(json!(12.34)),
            ])),
            val_str: "null",
            col_str: vec![
                "null".to_owned(),
                "true".to_owned(),
                "false".to_owned(),
                "123".to_owned(),
                "12.34".to_owned(),
            ],
        },
    ];

    let format = FormatSettings::default();
    for test in tests {
        let serializer = test.data_type.create_serializer(&test.column)?;
        let val_res = serializer.to_string_values(0, &format)?;
        assert_eq!(&val_res, test.val_str, "case: {:#?}", test.name);

        let mut col_res = vec![];
        for i in 0..test.column.len() {
            col_res.push(serializer.to_string_values(i, &format)?);
        }
        assert_eq!(col_res, test.col_str, "case: {:#?}", test.name);
    }

    {
        let data_type = StructType::create(
            Some(vec![
                "item_1".to_owned(),
                "item_2".to_owned(),
                "item_3".to_owned(),
                "item_4".to_owned(),
            ]),
            vec![
                Float64Type::new_impl(),
                StringType::new_impl(),
                BooleanType::new_impl(),
                DateType::new_impl(),
            ],
        );
        let column: ColumnRef = Arc::new(StructColumn::from_data(
            vec![
                Series::from_data(vec![1.2f64]),
                Series::from_data(vec!["hello"]),
                Series::from_data(vec![true]),
                Series::from_data(vec![18869i32]),
            ],
            DataTypeImpl::Struct(data_type),
        ));
        let serializer = column.data_type().create_serializer(&column)?;
        let result = serializer.to_string_values(0, &format)?;
        let expect = "(1.2, 'hello', 1, '2021-08-30')";
        assert_eq!(&result, expect);
    }

    Ok(())
}

#[test]
fn test_convert_arrow() {
    let t = TimestampType::new_impl();
    let arrow_y = t.to_arrow_field("x");
    let new_t = from_arrow_field(&arrow_y);

    assert_eq!(new_t.name(), t.name())
}

#[test]
fn test_enum_dispatch() -> Result<()> {
    let c = NullSerializer { size: 0 };
    let d: TypeSerializerImpl = c.into();
    let _: NullSerializer = d.try_into()?;
    Ok(())
}
