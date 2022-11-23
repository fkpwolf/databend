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

use chrono::DateTime;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::AddMonthsFunction;
use common_functions::scalars::AddTimesFunction;
use common_functions::scalars::FunctionContext;

#[test]
fn test_add_months() -> Result<()> {
    let dt_to_days = |dt: &str| -> i64 {
        DateTime::parse_from_rfc3339(dt).unwrap().timestamp() / (24 * 3600_i64)
    };

    let dt_to_microseconds =
        |dt: &str| -> i64 { DateTime::parse_from_rfc3339(dt).unwrap().timestamp_micros() };

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("date", DateType::new_impl()),
        DataField::new("datetime", TimestampType::new_impl()),
        DataField::new("u8", u8::to_data_type()),
        DataField::new("u16", u16::to_data_type()),
        DataField::new("u32", u32::to_data_type()),
        DataField::new("u64", u64::to_data_type()),
        DataField::new("i8", i8::to_data_type()),
        DataField::new("i16", i16::to_data_type()),
        DataField::new("i32", i32::to_data_type()),
        DataField::new("i64", i64::to_data_type()),
        DataField::new("f32", f32::to_data_type()),
        DataField::new("f64", f64::to_data_type()),
    ]);

    let blocks = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![dt_to_days("2020-02-29T10:00:00Z") as i32]),
        Series::from_data(vec![dt_to_microseconds("2020-02-29T01:02:03Z")]),
        Series::from_data(vec![12_u8]),
        Series::from_data(vec![12_u16]),
        Series::from_data(vec![12_u32]),
        Series::from_data(vec![12_u64]),
        Series::from_data(vec![-13_i8]),
        Series::from_data(vec![-13_i16]),
        Series::from_data(vec![-13_i32]),
        Series::from_data(vec![-13i64]),
        Series::from_data(vec![1.2_f32]),
        Series::from_data(vec![-1.2_f64]),
    ]);

    let column = |col_name: &str| -> ColumnWithField {
        ColumnWithField::new(
            blocks.try_column_by_name(col_name).unwrap().clone(),
            schema.field_with_name(col_name).unwrap().clone(),
        )
    };

    let fields = [
        "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64",
    ];
    let args = [
        &UInt8Type::new_impl(),
        &UInt16Type::new_impl(),
        &UInt32Type::new_impl(),
        &UInt64Type::new_impl(),
        &Int8Type::new_impl(),
        &Int16Type::new_impl(),
        &Int32Type::new_impl(),
        &Int64Type::new_impl(),
        &Float32Type::new_impl(),
        &Float64Type::new_impl(),
    ];

    {
        let mut expects: Vec<i32> = Vec::new();
        expects.reserve(10);
        for (field, arg) in fields.iter().zip(args.iter()) {
            let add_months =
                AddMonthsFunction::try_create_func("add_months", 1, &[&DateType::new_impl(), arg])?;
            let col = add_months.eval(
                FunctionContext::default(),
                &[column("date"), column(field)],
                1,
            )?;
            assert_eq!(col.len(), 1);
            assert_eq!(col.data_type().data_type_id(), TypeID::Int32);
            expects.push(col.get_i64(0)? as i32);
        }
        assert_eq!(expects, vec![
            dt_to_days("2021-02-28T10:00:00Z") as i32,
            dt_to_days("2021-02-28T10:00:00Z") as i32,
            dt_to_days("2021-02-28T10:00:00Z") as i32,
            dt_to_days("2021-02-28T10:00:00Z") as i32,
            dt_to_days("2019-01-29T10:00:00Z") as i32,
            dt_to_days("2019-01-29T10:00:00Z") as i32,
            dt_to_days("2019-01-29T10:00:00Z") as i32,
            dt_to_days("2019-01-29T10:00:00Z") as i32,
            dt_to_days("2020-03-29T10:00:00Z") as i32,
            dt_to_days("2020-01-29T10:00:00Z") as i32,
        ]);
    }

    {
        let mut expects: Vec<i64> = Vec::new();
        expects.reserve(10);
        for (field, arg) in fields.iter().zip(args.iter()) {
            let add_months = AddMonthsFunction::try_create_func("add_months", 1, &[
                &TimestampType::new_impl(),
                arg,
            ])?;
            let col = add_months.eval(
                FunctionContext::default(),
                &[column("datetime"), column(field)],
                1,
            )?;
            assert_eq!(col.len(), 1);
            assert_eq!(col.data_type().data_type_id(), TypeID::Int64);
            expects.push(col.get_i64(0)?);
        }
        assert_eq!(expects, vec![
            dt_to_microseconds("2021-02-28T01:02:03Z"),
            dt_to_microseconds("2021-02-28T01:02:03Z"),
            dt_to_microseconds("2021-02-28T01:02:03Z"),
            dt_to_microseconds("2021-02-28T01:02:03Z"),
            dt_to_microseconds("2019-01-29T01:02:03Z"),
            dt_to_microseconds("2019-01-29T01:02:03Z"),
            dt_to_microseconds("2019-01-29T01:02:03Z"),
            dt_to_microseconds("2019-01-29T01:02:03Z"),
            dt_to_microseconds("2020-03-29T01:02:03Z"),
            dt_to_microseconds("2020-01-29T01:02:03Z"),
        ]);
    }

    Ok(())
}

#[test]
fn test_add_subtract_seconds() -> Result<()> {
    let dt_to_seconds = |dt: &str| -> i64 { DateTime::parse_from_rfc3339(dt).unwrap().timestamp() };

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("datetime", TimestampType::new_impl()),
        DataField::new("u8", u8::to_data_type()),
        DataField::new("u16", u16::to_data_type()),
        DataField::new("u32", u32::to_data_type()),
        DataField::new("u64", u64::to_data_type()),
        DataField::new("i8", i8::to_data_type()),
        DataField::new("i16", i16::to_data_type()),
        DataField::new("i32", i32::to_data_type()),
        DataField::new("i64", i64::to_data_type()),
        DataField::new("f32", f32::to_data_type()),
        DataField::new("f64", f64::to_data_type()),
    ]);

    let blocks = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![dt_to_seconds("2020-02-29T23:59:59Z") * 1e6 as i64]),
        Series::from_data(vec![1_u8]),
        Series::from_data(vec![1_u16]),
        Series::from_data(vec![1_u32]),
        Series::from_data(vec![1_u64]),
        Series::from_data(vec![-1_i8]),
        Series::from_data(vec![-1_i16]),
        Series::from_data(vec![-1_i32]),
        Series::from_data(vec![-1_i64]),
        Series::from_data(vec![1.2_f32]),
        Series::from_data(vec![-1.2_f64]),
    ]);

    let column = |col_name: &str| -> ColumnWithField {
        ColumnWithField::new(
            blocks.try_column_by_name(col_name).unwrap().clone(),
            schema.field_with_name(col_name).unwrap().clone(),
        )
    };

    let fields = [
        "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64",
    ];
    let args = [
        &UInt8Type::new_impl(),
        &UInt16Type::new_impl(),
        &UInt32Type::new_impl(),
        &UInt64Type::new_impl(),
        &Int8Type::new_impl(),
        &Int16Type::new_impl(),
        &Int32Type::new_impl(),
        &Int64Type::new_impl(),
        &Float32Type::new_impl(),
        &Float64Type::new_impl(),
    ];

    {
        let mut expects: Vec<i64> = Vec::new();
        expects.reserve(10);
        for (field, arg) in fields.iter().zip(args.iter()) {
            let add_seconds = AddTimesFunction::try_create_func("add_seconds", 1, &[
                &TimestampType::new_impl(),
                arg,
            ])?;
            let col = add_seconds.eval(
                FunctionContext::default(),
                &[column("datetime"), column(field)],
                1,
            )?;
            assert_eq!(col.len(), 1);
            assert_eq!(col.data_type().data_type_id(), TypeID::Int64);
            expects.push(col.get_i64(0)? / 1e6 as i64);
        }
        assert_eq!(expects, vec![
            dt_to_seconds("2020-03-01T00:00:00Z"),
            dt_to_seconds("2020-03-01T00:00:00Z"),
            dt_to_seconds("2020-03-01T00:00:00Z"),
            dt_to_seconds("2020-03-01T00:00:00Z"),
            dt_to_seconds("2020-02-29T23:59:58Z"),
            dt_to_seconds("2020-02-29T23:59:58Z"),
            dt_to_seconds("2020-02-29T23:59:58Z"),
            dt_to_seconds("2020-02-29T23:59:58Z"),
            dt_to_seconds("2020-03-01T00:00:00Z"),
            dt_to_seconds("2020-02-29T23:59:58Z"),
        ]);
    }

    {
        let mut expects: Vec<i64> = Vec::new();
        expects.reserve(10);
        for (field, arg) in fields.iter().zip(args.iter()) {
            let add_seconds = AddTimesFunction::try_create_func("subtract_seconds", -1, &[
                &TimestampType::new_impl(),
                arg,
            ])?;
            let col = add_seconds.eval(
                FunctionContext::default(),
                &[column("datetime"), column(field)],
                1,
            )?;
            assert_eq!(col.len(), 1);
            assert_eq!(col.data_type().data_type_id(), TypeID::Int64);
            expects.push(col.get_i64(0)? / 1e6 as i64);
        }
        assert_eq!(expects, vec![
            dt_to_seconds("2020-02-29T23:59:58Z"),
            dt_to_seconds("2020-02-29T23:59:58Z"),
            dt_to_seconds("2020-02-29T23:59:58Z"),
            dt_to_seconds("2020-02-29T23:59:58Z"),
            dt_to_seconds("2020-03-01T00:00:00Z"),
            dt_to_seconds("2020-03-01T00:00:00Z"),
            dt_to_seconds("2020-03-01T00:00:00Z"),
            dt_to_seconds("2020-03-01T00:00:00Z"),
            dt_to_seconds("2020-02-29T23:59:58Z"),
            dt_to_seconds("2020-03-01T00:00:00Z"),
        ]);
    }

    Ok(())
}
