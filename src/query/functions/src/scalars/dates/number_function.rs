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

use std::fmt;
use std::marker::PhantomData;

use chrono::DateTime;
use chrono::Datelike;
use chrono::TimeZone;
use chrono::Timelike;
use chrono_tz::Tz;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use super::round_function::Round;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::scalar_unary_op;
use crate::scalars::CastFunction;
use crate::scalars::EvalContext;
use crate::scalars::FactoryCreator;
use crate::scalars::Function;
use crate::scalars::FunctionAdapter;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionFeatures;
use crate::scalars::Monotonicity;
use crate::scalars::RoundFunction;

#[derive(Clone, Debug)]
pub struct NumberFunction<T, R> {
    display_name: String,
    input_type: DataTypeImpl,
    t: PhantomData<T>,
    r: PhantomData<R>,
}

pub trait NumberOperator<R> {
    const IS_DETERMINISTIC: bool;

    fn to_number(_value: DateTime<Tz>, tz: &Tz) -> R;

    // Used to check the monotonicity of the function.
    // For example, ToDayOfYear is monotonous only when the time range is the same year.
    // So we can use ToStartOfYearFunction to check whether the time range is in the same year.
    // If the function always monotonous, just return error.
    fn factor_function(_input_type: DataTypeImpl) -> Option<Box<dyn Function>> {
        // default to "Always monotonous, has no factor function"
        None
    }

    fn return_type() -> Option<DataTypeImpl> {
        None
    }
}

#[derive(Clone)]
pub struct ToYYYYMM;

impl NumberOperator<u32> for ToYYYYMM {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, _tz: &Tz) -> u32 {
        value.year() as u32 * 100 + value.month()
    }
}

#[derive(Clone)]
pub struct ToYYYYMMDD;

impl NumberOperator<u32> for ToYYYYMMDD {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, _tz: &Tz) -> u32 {
        value.year() as u32 * 10000 + value.month() * 100 + value.day()
    }
}

#[derive(Clone)]
pub struct ToYYYYMMDDhhmmss;

impl NumberOperator<u64> for ToYYYYMMDDhhmmss {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, _tz: &Tz) -> u64 {
        value.year() as u64 * 10000000000
            + value.month() as u64 * 100000000
            + value.day() as u64 * 1000000
            + value.hour() as u64 * 10000
            + value.minute() as u64 * 100
            + value.second() as u64
    }
}

#[derive(Clone)]
pub struct ToStartOfYear;

impl NumberOperator<i32> for ToStartOfYear {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, tz: &Tz) -> i32 {
        let end = tz.ymd(value.year(), 1, 1).and_hms(0, 0, 0);
        get_day(end, tz) as i32
    }

    fn return_type() -> Option<DataTypeImpl> {
        Some(DateType::new_impl())
    }
}

#[derive(Clone)]
pub struct ToStartOfISOYear;

impl NumberOperator<i32> for ToStartOfISOYear {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, tz: &Tz) -> i32 {
        let week_day = value.weekday().num_days_from_monday();
        let iso_week = value.iso_week();
        let iso_week_num = iso_week.week();
        let sub_days = (iso_week_num - 1) * 7 + week_day;
        let result = value.timestamp_millis() - sub_days as i64 * 24 * 3600 * 1000;
        let end = tz.timestamp_millis(result);
        get_day(end, tz) as i32
    }

    fn return_type() -> Option<DataTypeImpl> {
        Some(DateType::new_impl())
    }
}

#[derive(Clone)]
pub struct ToStartOfQuarter;

impl NumberOperator<i32> for ToStartOfQuarter {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, tz: &Tz) -> i32 {
        let new_month = value.month0() / 3 * 3 + 1;
        let date = tz.ymd(value.year(), new_month, 1).and_hms(0, 0, 0);
        get_day(date, tz) as i32
    }

    fn return_type() -> Option<DataTypeImpl> {
        Some(DateType::new_impl())
    }
}

#[derive(Clone)]
pub struct ToStartOfMonth;

impl NumberOperator<i32> for ToStartOfMonth {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, tz: &Tz) -> i32 {
        let date = tz.ymd(value.year(), value.month(), 1).and_hms(0, 0, 0);
        get_day(date, tz) as i32
    }

    fn return_type() -> Option<DataTypeImpl> {
        Some(DateType::new_impl())
    }
}

#[derive(Clone)]
pub struct ToMonth;

impl NumberOperator<u8> for ToMonth {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, _tz: &Tz) -> u8 {
        value.month() as u8
    }

    // ToMonth is NOT a monotonic function in general, unless the time range is within the same year.
    // For example, date(2020-12-01) < date(2021-5-5), while ToMonth(2020-12-01) > ToMonth(2021-5-5).
    fn factor_function(input_type: DataTypeImpl) -> Option<Box<dyn Function>> {
        Some(ToStartOfYearFunction::try_create("toStartOfYear", input_type).unwrap())
    }
}

#[derive(Clone)]
pub struct ToDayOfYear;

impl NumberOperator<u16> for ToDayOfYear {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, _tz: &Tz) -> u16 {
        value.ordinal() as u16
    }

    // ToDayOfYear is NOT a monotonic function in general, unless the time range is within the same year.
    // For example, date(2020-12-01) < date(2021-5-5), while ToDayOfYear(2020-12-01) > ToDayOfYear(2021-5-5).
    fn factor_function(input_type: DataTypeImpl) -> Option<Box<dyn Function>> {
        Some(ToStartOfYearFunction::try_create("toStartOfYear", input_type).unwrap())
    }
}

#[derive(Clone)]
pub struct ToDayOfMonth;

impl NumberOperator<u8> for ToDayOfMonth {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, _tz: &Tz) -> u8 {
        value.day() as u8
    }

    // ToDayOfMonth is not a monotonic function in general, unless the time range is within the same month.
    // For example, date(2021-11-20) < date(2021-12-01), while ToDayOfMonth(2021-11-20) > ToDayOfMonth(2021-12-01).
    fn factor_function(input_type: DataTypeImpl) -> Option<Box<dyn Function>> {
        Some(ToStartOfMonthFunction::try_create("toStartOfMonth", input_type).unwrap())
    }
}

#[derive(Clone)]
pub struct ToDayOfWeek;

impl NumberOperator<u8> for ToDayOfWeek {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, _tz: &Tz) -> u8 {
        value.weekday().number_from_monday() as u8
    }

    // ToDayOfWeek is NOT a monotonic function in general, unless the time range is within the same week.
    fn factor_function(input_type: DataTypeImpl) -> Option<Box<dyn Function>> {
        Some(ToMondayFunction::try_create("toMonday", input_type).unwrap())
    }
}

#[derive(Clone)]
pub struct ToHour;

impl NumberOperator<u8> for ToHour {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, _tz: &Tz) -> u8 {
        value.hour() as u8
    }

    // ToHour is NOT a monotonic function in general, unless the time range is within the same day.
    fn factor_function(_input_type: DataTypeImpl) -> Option<Box<dyn Function>> {
        let type_name = DateType::new_impl().name();
        Some(CastFunction::create("toDate", type_name.as_str(), u8::to_data_type()).unwrap())
    }
}

#[derive(Clone)]
pub struct ToMinute;

impl NumberOperator<u8> for ToMinute {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, _tz: &Tz) -> u8 {
        value.minute() as u8
    }

    // ToMinute is NOT a monotonic function in general, unless the time range is within the same hour.
    fn factor_function(_input_type: DataTypeImpl) -> Option<Box<dyn Function>> {
        Some(
            RoundFunction::try_create("toStartOfHour", &[&TimestampType::new_impl()], Round::Hour)
                .unwrap(),
        )
    }
}

#[derive(Clone)]
pub struct ToSecond;

impl NumberOperator<u8> for ToSecond {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, _tz: &Tz) -> u8 {
        value.second() as u8
    }

    // ToSecond is NOT a monotonic function in general, unless the time range is within the same minute.
    fn factor_function(_input_type: DataTypeImpl) -> Option<Box<dyn Function>> {
        Some(
            RoundFunction::try_create(
                "to_start_of_minute",
                &[&TimestampType::new_impl()],
                Round::Minute,
            )
            .unwrap(),
        )
    }
}

#[derive(Clone)]
pub struct ToMonday;

impl NumberOperator<i32> for ToMonday {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, tz: &Tz) -> i32 {
        let weekday = value.weekday();
        (get_day(value, tz) as u32 - weekday.num_days_from_monday()) as i32
    }

    fn return_type() -> Option<DataTypeImpl> {
        Some(DateType::new_impl())
    }
}

#[derive(Clone)]
pub struct ToYear;

impl NumberOperator<u16> for ToYear {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Tz>, _tz: &Tz) -> u16 {
        value.year() as u16
    }
}

impl<T, R> NumberFunction<T, R>
where
    T: NumberOperator<R> + Clone + Sync + Send + 'static,
    R: PrimitiveType + Clone + ToDataType + Scalar<RefType<'static> = R>,
{
    pub fn try_create(display_name: &str, input_type: DataTypeImpl) -> Result<Box<dyn Function>> {
        Ok(Box::new(NumberFunction::<T, R> {
            display_name: display_name.to_string(),
            input_type,
            t: PhantomData,
            r: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        let mut features = FunctionFeatures::default().monotonicity().num_arguments(1);

        if T::IS_DETERMINISTIC {
            features = features.deterministic();
        }

        let function_creator: FactoryCreator =
            Box::new(move |display_name, args| Self::try_create(display_name, args[0].clone()));

        FunctionDescription::creator(function_creator).features(features)
    }
}

impl<T, R> Function for NumberFunction<T, R>
where
    T: NumberOperator<R> + Clone + Sync + Send,
    R: PrimitiveType + Clone + ToDataType,
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self) -> DataTypeImpl {
        match T::return_type() {
            None => R::to_data_type(),
            Some(v) => v,
        }
    }

    fn eval(
        &self,
        func_ctx: FunctionContext,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let type_id = self.input_type.data_type_id();

        let number_array = match type_id {
            TypeID::Date => {
                let tz = "UTC".parse::<Tz>().unwrap();
                let func = |v: i32, _ctx: &mut EvalContext| {
                    let date_time = tz.timestamp(v as i64 * 24 * 3600, 0_u32);
                    T::to_number(date_time, &tz)
                };
                let col = scalar_unary_op::<i32, R, _>(
                    columns[0].column(),
                    func,
                    &mut EvalContext::default(),
                )?;
                Ok(col.arc())
            }
            TypeID::Timestamp => {
                // round_func need to calcute it with origin timezone
                // such as in UTC: 2022-03-31 22:00 and in +8:00 time is 2022-04-01 6:00
                // then the result of to the month of should be 2022-04-01 6:00 rather than 2022-03-01 22:00
                let tz = func_ctx.tz;
                let func = |v: i64, _ctx: &mut EvalContext| {
                    let date_time = tz.timestamp(v / 1_000_000, 0_u32);
                    T::to_number(date_time, &tz)
                };
                let col = scalar_unary_op::<i64, R, _>(
                    columns[0].column(),
                    func,
                    &mut EvalContext::default(),
                )?;
                Ok(col.arc())
            }
            other => Result::Err(ErrorCode::IllegalDataType(format!(
                "Illegal type {:?} of argument of function {}.Should be a date/datetime",
                other,
                self.name()
            ))),
        }?;
        Ok(number_array)
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        let func = match T::factor_function(self.input_type.clone()) {
            Some(f) => f,
            // Always monotonous, has no factor function.
            None => return Ok(Monotonicity::clone_without_range(&args[0])),
        };

        let func = FunctionAdapter::create(func, true);

        if args[0].left.is_none() || args[0].right.is_none() {
            return Ok(Monotonicity::default());
        }

        let left_val = func
            .eval(
                FunctionContext::default(),
                &[args[0].left.clone().unwrap()],
                1,
            )?
            .get(0);
        let right_val = func
            .eval(
                FunctionContext::default(),
                &[args[0].right.clone().unwrap()],
                1,
            )?
            .get(0);
        // The function is monotonous, if the factor eval returns the same values for them.
        if left_val == right_val {
            return Ok(Monotonicity::clone_without_range(&args[0]));
        }

        Ok(Monotonicity::default())
    }
}

impl<T, R> fmt::Display for NumberFunction<T, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

fn get_day(date: DateTime<Tz>, tz: &Tz) -> i64 {
    let start = tz.ymd(1970, 1, 1).and_hms(0, 0, 0);
    let duration = date.signed_duration_since(start);
    duration.num_days()
}

pub type ToYYYYMMFunction = NumberFunction<ToYYYYMM, u32>;
pub type ToYYYYMMDDFunction = NumberFunction<ToYYYYMMDD, u32>;
pub type ToYYYYMMDDhhmmssFunction = NumberFunction<ToYYYYMMDDhhmmss, u64>;

pub type ToStartOfISOYearFunction = NumberFunction<ToStartOfISOYear, i32>;
pub type ToStartOfYearFunction = NumberFunction<ToStartOfYear, i32>;
pub type ToStartOfQuarterFunction = NumberFunction<ToStartOfQuarter, i32>;
pub type ToStartOfMonthFunction = NumberFunction<ToStartOfMonth, i32>;

pub type ToMonthFunction = NumberFunction<ToMonth, u8>;
pub type ToDayOfYearFunction = NumberFunction<ToDayOfYear, u16>;
pub type ToDayOfMonthFunction = NumberFunction<ToDayOfMonth, u8>;
pub type ToDayOfWeekFunction = NumberFunction<ToDayOfWeek, u8>;

pub type ToHourFunction = NumberFunction<ToHour, u8>;
pub type ToMinuteFunction = NumberFunction<ToMinute, u8>;
pub type ToSecondFunction = NumberFunction<ToSecond, u8>;

pub type ToMondayFunction = NumberFunction<ToMonday, i32>;

pub type ToYearFunction = NumberFunction<ToYear, u16>;
