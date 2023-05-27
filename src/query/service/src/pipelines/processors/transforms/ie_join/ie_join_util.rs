// Copyright 2021 Datafuse Labs
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

use std::cmp::min;
use std::cmp::Ordering;

use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::executor::cast_expr_to_non_null_boolean;

pub(crate) fn filter_block(block: DataBlock, filter: &RemoteExpr) -> Result<DataBlock> {
    let filter = filter.as_expr(&BUILTIN_FUNCTIONS);
    let other_predicate = cast_expr_to_non_null_boolean(filter)?;
    assert_eq!(other_predicate.data_type(), &DataType::Boolean);

    let func_ctx = FunctionContext::default();

    let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
    let predicate = evaluator
        .run(&other_predicate)?
        .try_downcast::<BooleanType>()
        .unwrap();
    block.filter_boolean_value(&predicate)
}

pub(crate) fn order_match(op: &str, order: Ordering) -> bool {
    match op {
        "gt" => order == Ordering::Greater,
        "gte" => order == Ordering::Equal || order == Ordering::Greater,
        "lt" => order == Ordering::Less,
        "lte" => order == Ordering::Less || order == Ordering::Equal,
        _ => unreachable!(),
    }
}

// Exponential search
pub(crate) fn probe_l1(l1: &Column, pos: usize, op1: &str) -> usize {
    let mut step = 1;
    let n = l1.len();
    let mut hi = pos;
    let mut lo = pos;
    let mut off1;
    if matches!(op1, "gte" | "lte") {
        lo -= min(step, lo);
        step *= 2;
        off1 = lo;
        while lo > 0
            && order_match(
                op1,
                unsafe { l1.index_unchecked(pos) }.cmp(&unsafe { l1.index_unchecked(off1) }),
            )
        {
            hi = lo;
            lo -= min(step, lo);
            step *= 2;
            off1 = lo;
        }
    } else {
        hi += min(step, n - hi);
        step *= 2;
        off1 = hi;
        while hi < n
            && !order_match(
                op1,
                unsafe { l1.index_unchecked(pos) }.cmp(&unsafe { l1.index_unchecked(off1) }),
            )
        {
            lo = hi;
            hi += min(step, n - hi);
            step *= 2;
            off1 = hi;
        }
    }
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        off1 = mid;
        if order_match(
            op1,
            unsafe { l1.index_unchecked(pos) }.cmp(&unsafe { l1.index_unchecked(off1) }),
        ) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}
