// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::arithmetics::ArithmeticFunction;
use crate::datavalues::DataValueArithmeticOperator;
use crate::{FunctionResult, IFunction};

pub struct ArithmeticPlusFunction;

impl ArithmeticPlusFunction {
    pub fn try_create_func(args: &[Box<dyn IFunction>]) -> FunctionResult<Box<dyn IFunction>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Plus, args)
    }
}
