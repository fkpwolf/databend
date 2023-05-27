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

use common_expression::Expr;
use common_expression::Scalar;

pub fn find_eq_filter(expr: &Expr<String>, visitor: &mut impl FnMut(&str, &Scalar)) {
    match expr {
        Expr::Constant { .. } | Expr::ColumnRef { .. } => {}
        Expr::Cast { expr, .. } => find_eq_filter(expr, visitor),
        Expr::FunctionCall { function, args, .. } => {
            if function.signature.name == "eq" {
                match args.as_slice() {
                    [Expr::ColumnRef { id, .. }, Expr::Constant { scalar, .. }]
                    | [Expr::Constant { scalar, .. }, Expr::ColumnRef { id, .. }] => {
                        visitor(id, scalar);
                    }
                    _ => {}
                }
            } else if function.signature.name == "and_filters" {
                // only support this:
                // 1. where xx and xx and xx
                // 2. filter: Column `table`, Column `database`
                for arg in args {
                    find_eq_filter(arg, visitor)
                }
            }
        }
    }
}
