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

use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::Query;
use common_ast::ast::SelectStmt;
use common_ast::ast::SetExpr;
use common_ast::ast::TableReference;
use common_ast::VisitorMut;

#[derive(Debug, Clone, Default)]
pub struct DistinctToGroupBy {}

impl VisitorMut for DistinctToGroupBy {
    fn visit_select_stmt(&mut self, stmt: &mut SelectStmt<'_>) {
        let SelectStmt {
            select_list,
            from,
            selection,
            group_by,
            having,
            ..
        } = stmt;

        if group_by.is_empty() && select_list.len() == 1 && from.len() == 1 {
            if let common_ast::ast::SelectTarget::AliasedExpr {
                expr:
                    box Expr::FunctionCall {
                        span,
                        distinct,
                        name,
                        args,
                        ..
                    },
                alias,
            } = &select_list[0]
            {
                if ((name.name.to_ascii_lowercase() == "count" && *distinct)
                    || name.name.to_ascii_lowercase() == "count_distinct")
                    && args.iter().all(|arg| !matches!(arg, Expr::Literal { .. }))
                {
                    let tmp_token = span[0].clone();
                    let subquery = Query {
                        span: &[],
                        with: None,
                        body: SetExpr::Select(Box::new(SelectStmt {
                            span: &[],
                            distinct: false,
                            select_list: vec![],
                            from: from.clone(),
                            selection: None,
                            group_by: args.clone(),
                            having: None,
                        })),
                        order_by: vec![],
                        limit: vec![],
                        offset: None,
                        ignore_result: false,
                    };

                    let new_stmt = SelectStmt {
                        span: &[],
                        distinct: false,
                        select_list: vec![common_ast::ast::SelectTarget::AliasedExpr {
                            expr: Box::new(Expr::FunctionCall {
                                span: &[],
                                distinct: false,
                                name: Identifier {
                                    name: "count".to_string(),
                                    quote: None,
                                    span: tmp_token.clone(),
                                },
                                args: vec![],
                                params: vec![],
                            }),
                            alias: alias.clone(),
                        }],
                        from: vec![TableReference::Subquery {
                            span: &[],
                            subquery: Box::new(subquery),
                            alias: None,
                        }],
                        selection: selection.clone(),
                        group_by: vec![],
                        having: having.clone(),
                    };

                    *stmt = new_stmt;
                }
            }
        }
    }
}
