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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::catalog::CatalogManager;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::plan::PrewhereInfo;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_function;
use common_expression::types::DataType;
use common_expression::ConstantFolder;
use common_expression::DataBlock;
use common_expression::DataSchemaRefExt;
use common_expression::Expr;
use common_expression::RemoteExpr;
use common_expression::TableSchema;
use common_functions::scalars::BUILTIN_FUNCTIONS;

use super::cast_expr_to_non_null_boolean;
use super::AggregateExpand;
use super::AggregateFinal;
use super::AggregateFunctionDesc;
use super::AggregateFunctionSignature;
use super::AggregatePartial;
use super::Exchange as PhysicalExchange;
use super::Filter;
use super::HashJoin;
use super::Limit;
use super::Sort;
use super::TableScan;
use super::Unnest;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::table_read_plan::ToReadDataSourcePlan;
use crate::executor::EvalScalar;
use crate::executor::FragmentKind;
use crate::executor::PhysicalPlan;
use crate::executor::RuntimeFilterSource;
use crate::executor::SortDesc;
use crate::executor::UnionAll;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::AggregateMode;
use crate::plans::AndExpr;
use crate::plans::Exchange;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::Scan;
use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::DerivedColumn;
use crate::Metadata;
use crate::MetadataRef;
use crate::TableInternalColumn;
use crate::DUMMY_COLUMN_INDEX;
use crate::DUMMY_TABLE_INDEX;

pub struct PhysicalPlanBuilder {
    metadata: MetadataRef,
    ctx: Arc<dyn TableContext>,
    next_plan_id: u32,
}

impl PhysicalPlanBuilder {
    pub fn new(metadata: MetadataRef, ctx: Arc<dyn TableContext>) -> Self {
        Self {
            metadata,
            ctx,
            next_plan_id: 0,
        }
    }

    fn next_plan_id(&mut self) -> u32 {
        let id = self.next_plan_id;
        self.next_plan_id += 1;
        id
    }

    fn build_projection(
        metadata: &Metadata,
        schema: &TableSchema,
        columns: &ColumnSet,
        has_inner_column: bool,
        ignore_internal_column: bool,
    ) -> Projection {
        if !has_inner_column {
            let mut col_indices = Vec::new();
            for index in columns.iter() {
                let name = match metadata.column(*index) {
                    ColumnEntry::BaseTableColumn(BaseTableColumn { column_name, .. }) => {
                        column_name
                    }
                    ColumnEntry::DerivedColumn(DerivedColumn { alias, .. }) => alias,
                    ColumnEntry::InternalColumn(TableInternalColumn {
                        internal_column, ..
                    }) => {
                        if ignore_internal_column {
                            continue;
                        }
                        internal_column.column_name()
                    }
                };
                col_indices.push(schema.index_of(name).unwrap());
            }

            col_indices.sort();
            Projection::Columns(col_indices)
        } else {
            let mut col_indices = BTreeMap::new();
            for index in columns.iter() {
                let column = metadata.column(*index);
                match column {
                    ColumnEntry::BaseTableColumn(BaseTableColumn {
                        column_name,
                        path_indices,
                        ..
                    }) => match path_indices {
                        Some(path_indices) => {
                            col_indices.insert(column.index(), path_indices.to_vec());
                        }
                        None => {
                            let idx = schema.index_of(column_name).unwrap();
                            col_indices.insert(column.index(), vec![idx]);
                        }
                    },
                    ColumnEntry::DerivedColumn(DerivedColumn { alias, .. }) => {
                        let idx = schema.index_of(alias).unwrap();
                        col_indices.insert(column.index(), vec![idx]);
                    }
                    ColumnEntry::InternalColumn(TableInternalColumn { column_index, .. }) => {
                        if !ignore_internal_column {
                            col_indices.insert(*column_index, vec![*column_index]);
                        }
                    }
                }
            }
            Projection::InnerColumns(col_indices)
        }
    }

    #[async_recursion::async_recursion]
    pub async fn build(&mut self, s_expr: &SExpr) -> Result<PhysicalPlan> {
        // Build stat info
        let stat_info = self.build_plan_stat_info(s_expr)?;

        match s_expr.plan() {
            RelOperator::Scan(scan) => {
                let mut has_inner_column = false;
                let mut name_mapping = BTreeMap::new();
                let mut project_internal_columns = BTreeMap::new();
                let metadata = self.metadata.read().clone();
                for index in scan.columns.iter() {
                    let column = metadata.column(*index);
                    if let ColumnEntry::BaseTableColumn(BaseTableColumn { path_indices, .. }) =
                        column
                    {
                        if path_indices.is_some() {
                            has_inner_column = true;
                        }
                    } else if let ColumnEntry::InternalColumn(TableInternalColumn {
                        internal_column,
                        ..
                    }) = column
                    {
                        project_internal_columns.insert(*index, internal_column.to_owned());
                    }

                    let name = match column {
                        ColumnEntry::BaseTableColumn(BaseTableColumn { column_name, .. }) => {
                            column_name
                        }
                        ColumnEntry::DerivedColumn(DerivedColumn { alias, .. }) => alias,
                        ColumnEntry::InternalColumn(TableInternalColumn {
                            internal_column,
                            ..
                        }) => internal_column.column_name(),
                    };
                    if let Some(prewhere) = &scan.prewhere {
                        // if there is a prewhere optimization,
                        // we can prune `PhysicalScan`'s output schema.
                        if prewhere.output_columns.contains(index) {
                            name_mapping.insert(name.to_string(), *index);
                        }
                    } else {
                        name_mapping.insert(name.to_string(), *index);
                    }
                }

                let table_entry = metadata.table(scan.table_index);
                let table = table_entry.table();
                let mut table_schema = table.schema();
                if !project_internal_columns.is_empty() {
                    let mut schema = table_schema.as_ref().clone();
                    for internal_column in project_internal_columns.values() {
                        schema.add_internal_column(
                            internal_column.column_name(),
                            internal_column.table_data_type(),
                            internal_column.column_id(),
                        );
                    }
                    table_schema = Arc::new(schema);
                }

                let push_downs = self.push_downs(scan, &table_schema, has_inner_column)?;

                let source = table
                    .read_plan_with_catalog(
                        self.ctx.clone(),
                        table_entry.catalog().to_string(),
                        Some(push_downs),
                        if project_internal_columns.is_empty() {
                            None
                        } else {
                            Some(project_internal_columns.clone())
                        },
                    )
                    .await?;

                let internal_column = if project_internal_columns.is_empty() {
                    None
                } else {
                    Some(project_internal_columns)
                };
                Ok(PhysicalPlan::TableScan(TableScan {
                    plan_id: self.next_plan_id(),
                    name_mapping,
                    source: Box::new(source),
                    table_index: scan.table_index,
                    stat_info: Some(stat_info),
                    internal_column,
                }))
            }
            RelOperator::DummyTableScan(_) => {
                let catalogs = CatalogManager::instance();
                let table = catalogs
                    .get_catalog(CATALOG_DEFAULT)?
                    .get_table(self.ctx.get_tenant().as_str(), "system", "one")
                    .await?;
                let source = table
                    .read_plan_with_catalog(
                        self.ctx.clone(),
                        CATALOG_DEFAULT.to_string(),
                        None,
                        None,
                    )
                    .await?;
                Ok(PhysicalPlan::TableScan(TableScan {
                    plan_id: self.next_plan_id(),
                    name_mapping: BTreeMap::from([("dummy".to_string(), DUMMY_COLUMN_INDEX)]),
                    source: Box::new(source),
                    table_index: DUMMY_TABLE_INDEX,
                    stat_info: Some(PlanStatsInfo {
                        estimated_rows: 1.0,
                    }),
                    internal_column: None,
                }))
            }
            RelOperator::Join(join) => {
                let build_side = self.build(s_expr.child(1)?).await?;
                let probe_side = self.build(s_expr.child(0)?).await?;
                let build_schema = build_side.output_schema()?;
                let probe_schema = probe_side.output_schema()?;
                let merged_schema = DataSchemaRefExt::create(
                    probe_schema
                        .fields()
                        .iter()
                        .chain(build_schema.fields())
                        .cloned()
                        .collect::<Vec<_>>(),
                );
                Ok(PhysicalPlan::HashJoin(HashJoin {
                    plan_id: self.next_plan_id(),
                    build: Box::new(build_side),
                    probe: Box::new(probe_side),
                    join_type: join.join_type.clone(),
                    build_keys: join
                        .right_conditions
                        .iter()
                        .map(|scalar| {
                            let expr =
                                scalar
                                    .as_expr_with_col_index()?
                                    .project_column_ref(|index| {
                                        build_schema.index_of(&index.to_string()).unwrap()
                                    });
                            let (expr, _) = ConstantFolder::fold(
                                &expr,
                                self.ctx.get_function_context()?,
                                &BUILTIN_FUNCTIONS,
                            );
                            Ok(expr.as_remote_expr())
                        })
                        .collect::<Result<_>>()?,
                    probe_keys: join
                        .left_conditions
                        .iter()
                        .map(|scalar| {
                            let expr =
                                scalar
                                    .as_expr_with_col_index()?
                                    .project_column_ref(|index| {
                                        probe_schema.index_of(&index.to_string()).unwrap()
                                    });
                            let (expr, _) = ConstantFolder::fold(
                                &expr,
                                self.ctx.get_function_context()?,
                                &BUILTIN_FUNCTIONS,
                            );
                            Ok(expr.as_remote_expr())
                        })
                        .collect::<Result<_>>()?,
                    non_equi_conditions: join
                        .non_equi_conditions
                        .iter()
                        .map(|scalar| {
                            let expr =
                                scalar
                                    .as_expr_with_col_index()?
                                    .project_column_ref(|index| {
                                        merged_schema.index_of(&index.to_string()).unwrap()
                                    });
                            let (expr, _) = ConstantFolder::fold(
                                &expr,
                                self.ctx.get_function_context()?,
                                &BUILTIN_FUNCTIONS,
                            );
                            Ok(expr.as_remote_expr())
                        })
                        .collect::<Result<_>>()?,
                    marker_index: join.marker_index,
                    from_correlated_subquery: join.from_correlated_subquery,

                    contain_runtime_filter: join.contain_runtime_filter,
                    stat_info: Some(stat_info),
                }))
            }

            RelOperator::EvalScalar(eval_scalar) => {
                // If there is `unnest` in `eval_scalar`, we should split the physical plan into three parts:
                // 3. Eval After Unnest Scalar
                // |_______2. Unnest
                //         |_______1. Eval Before Unnest Scalar
                // The input fields and the output fields of each part will be like:
                // 1. [i1, i2, .., in] -> [i1, i2, .. in, b1, b2, .., bm] (m == before_unnest.len() == unnest.len())
                // 2. [i1, i2, .. in, b1, b2, .., bm] -> [i1, i2, .. in, u1, u2, .., um] (`unnest` will replace the columns in place)
                // 3. [i1, i2, .. in, u1, u2, .., un] -> [i1, i2, .. in, u1, u2, .., um, o1, o2, .., op] (p == after_unnest.len())

                let input = Box::new(self.build(s_expr.child(0)?).await?);
                let input_schema = input.output_schema()?;

                let mut before_exprs = vec![];
                let mut unnest_offset = input_schema.fields().len();

                // 1. Collect the before unnest scalars, unnest, and after unnest scalars.
                let after_exprs = eval_scalar
                    .items
                    .iter()
                    .map(|item| {
                        // 1.1 Collect the before unnest scalars, and build into `RemoteExpr`.
                        let mut before_scalars = vec![];
                        item.scalar
                            .collect_before_unnest_scalars(&mut before_scalars);
                        let before =
                            before_scalars
                                .iter()
                                .map(|scalar| {
                                    let expr = scalar.as_expr_with_col_index()?.project_column_ref(
                                        |index| input_schema.index_of(&index.to_string()).unwrap(),
                                    );
                                    let (expr, _) = ConstantFolder::fold(
                                        &expr,
                                        self.ctx.get_function_context()?,
                                        &BUILTIN_FUNCTIONS,
                                    );
                                    Ok((expr.as_remote_expr(), item.index))
                                })
                                .collect::<Result<Vec<_>>>()?;
                        before_exprs.extend(before);

                        // 1.2 Collect the after unnest scalars, and build into `RemoteExpr`.
                        let expr = item
                            .scalar
                            .as_expr_with_col_index()?
                            .project_column_ref_with_unnest_offset(
                                |index| input_schema.index_of(&index.to_string()).unwrap(),
                                &mut unnest_offset,
                            );
                        let (expr, _) = ConstantFolder::fold(
                            &expr,
                            self.ctx.get_function_context()?,
                            &BUILTIN_FUNCTIONS,
                        );

                        debug_assert!(
                            unnest_offset == before_exprs.len() + input_schema.fields().len()
                        );
                        Ok((expr.as_remote_expr(), item.index))
                    })
                    .collect::<Result<_>>()?;

                // 2. Construct the physical plan.
                Ok(if before_exprs.is_empty() {
                    PhysicalPlan::EvalScalar(EvalScalar {
                        plan_id: self.next_plan_id(),
                        input,
                        exprs: after_exprs,
                        stat_info: Some(stat_info.clone()),
                    })
                } else {
                    let num_unnest_columns = before_exprs.len();
                    let before_plan = PhysicalPlan::EvalScalar(EvalScalar {
                        plan_id: self.next_plan_id(),
                        input,
                        exprs: before_exprs,
                        stat_info: Some(stat_info.clone()),
                    });
                    let unnest_plan = PhysicalPlan::Unnest(Unnest {
                        plan_id: self.next_plan_id(),
                        input: Box::new(before_plan),
                        num_columns: num_unnest_columns,
                        stat_info: Some(stat_info.clone()),
                    });
                    PhysicalPlan::EvalScalar(EvalScalar {
                        plan_id: self.next_plan_id(),
                        input: Box::new(unnest_plan),
                        exprs: after_exprs,
                        stat_info: Some(stat_info),
                    })
                })
            }

            RelOperator::Filter(filter) => {
                let input = Box::new(self.build(s_expr.child(0)?).await?);
                let input_schema = input.output_schema()?;
                Ok(PhysicalPlan::Filter(Filter {
                    plan_id: self.next_plan_id(),
                    input,
                    predicates: filter
                        .predicates
                        .iter()
                        .map(|scalar| {
                            let expr =
                                scalar
                                    .as_expr_with_col_index()?
                                    .project_column_ref(|index| {
                                        input_schema.index_of(&index.to_string()).unwrap()
                                    });
                            let expr = cast_expr_to_non_null_boolean(expr)?;
                            let (expr, _) = ConstantFolder::fold(
                                &expr,
                                self.ctx.get_function_context()?,
                                &BUILTIN_FUNCTIONS,
                            );
                            Ok(expr.as_remote_expr())
                        })
                        .collect::<Result<_>>()?,

                    stat_info: Some(stat_info),
                }))
            }
            RelOperator::Aggregate(agg) => {
                let input = self.build(s_expr.child(0)?).await?;
                let input_schema = input.output_schema()?;
                let group_items = agg.group_items.iter().map(|v| v.index).collect::<Vec<_>>();

                let result = match &agg.mode {
                    AggregateMode::Partial => {
                        let agg_funcs: Vec<AggregateFunctionDesc> = agg.aggregate_functions.iter().map(|v| {
                            if let ScalarExpr::AggregateFunction(agg) = &v.scalar {
                                Ok(AggregateFunctionDesc {
                                    sig: AggregateFunctionSignature {
                                        name: agg.func_name.clone(),
                                        args: agg.args.iter().map(|s| {
                                            s.data_type()
                                        }).collect::<Result<_>>()?,
                                        params: agg.params.clone(),
                                        return_type: *agg.return_type.clone(),
                                    },
                                    output_column: v.index,
                                    args: agg.args.iter().map(|arg| {
                                        if let ScalarExpr::BoundColumnRef(col) = arg {
                                            input_schema.index_of(&col.column.index.to_string())
                                        } else {
                                            Err(ErrorCode::Internal(
                                                "Aggregate function argument must be a BoundColumnRef".to_string()
                                            ))
                                        }
                                    }).collect::<Result<_>>()?,
                                    arg_indices: agg.args.iter().map(|arg| {
                                        if let ScalarExpr::BoundColumnRef(col) = arg {
                                            Ok(col.column.index)
                                        } else {
                                            Err(ErrorCode::Internal(
                                                "Aggregate function argument must be a BoundColumnRef".to_string()
                                            ))
                                        }
                                    }).collect::<Result<_>>()?,
                                })
                            } else {
                                Err(ErrorCode::Internal("Expected aggregate function".to_string()))
                            }
                        }).collect::<Result<_>>()?;

                        match input {
                            PhysicalPlan::Exchange(PhysicalExchange { input, kind, .. }) => {
                                let aggregate_partial = if !agg.grouping_sets.is_empty() {
                                    let expand = AggregateExpand {
                                        plan_id: self.next_plan_id(),
                                        input,
                                        group_bys: group_items.clone(),
                                        grouping_id_index: agg.grouping_id_index,
                                        grouping_sets: agg.grouping_sets.clone(),
                                        stat_info: Some(stat_info.clone()),
                                    };
                                    AggregatePartial {
                                        plan_id: self.next_plan_id(),
                                        input: Box::new(PhysicalPlan::AggregateExpand(expand)),
                                        agg_funcs,
                                        group_by: group_items,
                                        stat_info: Some(stat_info),
                                    }
                                } else {
                                    AggregatePartial {
                                        plan_id: self.next_plan_id(),
                                        input,
                                        agg_funcs,
                                        group_by: group_items,
                                        stat_info: Some(stat_info),
                                    }
                                };

                                let group_by_key_index =
                                    aggregate_partial.output_schema()?.num_fields() - 1;
                                let group_by_key_data_type =
                                    DataBlock::choose_hash_method_with_types(
                                        &agg.group_items
                                            .iter()
                                            .map(|v| v.scalar.data_type())
                                            .collect::<Result<Vec<_>>>()?,
                                    )?
                                    .data_type();

                                PhysicalPlan::Exchange(PhysicalExchange {
                                    kind,
                                    input: Box::new(PhysicalPlan::AggregatePartial(
                                        aggregate_partial,
                                    )),
                                    keys: vec![RemoteExpr::ColumnRef {
                                        span: None,
                                        id: group_by_key_index,
                                        data_type: group_by_key_data_type,
                                        display_name: "_group_by_key".to_string(),
                                    }],
                                })
                            }
                            _ => {
                                if !agg.grouping_sets.is_empty() {
                                    let expand = AggregateExpand {
                                        plan_id: self.next_plan_id(),
                                        input: Box::new(input),
                                        group_bys: group_items.clone(),
                                        grouping_id_index: agg.grouping_id_index,
                                        grouping_sets: agg.grouping_sets.clone(),
                                        stat_info: Some(stat_info.clone()),
                                    };
                                    PhysicalPlan::AggregatePartial(AggregatePartial {
                                        plan_id: self.next_plan_id(),
                                        agg_funcs,
                                        group_by: group_items,
                                        input: Box::new(PhysicalPlan::AggregateExpand(expand)),
                                        stat_info: Some(stat_info),
                                    })
                                } else {
                                    PhysicalPlan::AggregatePartial(AggregatePartial {
                                        plan_id: self.next_plan_id(),
                                        agg_funcs,
                                        group_by: group_items,
                                        input: Box::new(input),
                                        stat_info: Some(stat_info),
                                    })
                                }
                            }
                        }
                    }

                    // Hack to get before group by schema, we should refactor this
                    AggregateMode::Final => {
                        let input_schema = match input {
                            PhysicalPlan::AggregatePartial(ref agg) => agg.input.output_schema()?,

                            PhysicalPlan::Exchange(PhysicalExchange {
                                input: box PhysicalPlan::AggregatePartial(ref agg),
                                ..
                            }) => agg.input.output_schema()?,

                            _ => {
                                return Err(ErrorCode::Internal(format!(
                                    "invalid input physical plan: {}",
                                    input.name(),
                                )));
                            }
                        };

                        let agg_funcs: Vec<AggregateFunctionDesc> = agg.aggregate_functions.iter().map(|v| {
                            if let ScalarExpr::AggregateFunction(agg) = &v.scalar {
                                Ok(AggregateFunctionDesc {
                                    sig: AggregateFunctionSignature {
                                        name: agg.func_name.clone(),
                                        args: agg.args.iter().map(|s| {
                                            s.data_type()
                                        }).collect::<Result<_>>()?,
                                        params: agg.params.clone(),
                                        return_type: *agg.return_type.clone(),
                                    },
                                    output_column: v.index,
                                    args: agg.args.iter().map(|arg| {
                                        if let ScalarExpr::BoundColumnRef(col) = arg {
                                            input_schema.index_of(&col.column.index.to_string())
                                        } else {
                                            Err(ErrorCode::Internal(
                                                "Aggregate function argument must be a BoundColumnRef".to_string()
                                            ))
                                        }
                                    }).collect::<Result<_>>()?,
                                    arg_indices: agg.args.iter().map(|arg| {
                                        if let ScalarExpr::BoundColumnRef(col) = arg {
                                            Ok(col.column.index)
                                        } else {
                                            Err(ErrorCode::Internal(
                                                "Aggregate function argument must be a BoundColumnRef".to_string()
                                            ))
                                        }
                                    }).collect::<Result<_>>()?,
                                })
                            } else {
                                Err(ErrorCode::Internal("Expected aggregate function".to_string()))
                            }
                        }).collect::<Result<_>>()?;

                        match input {
                            PhysicalPlan::AggregatePartial(ref partial) => {
                                let before_group_by_schema = partial.input.output_schema()?;
                                let limit = agg.limit;
                                PhysicalPlan::AggregateFinal(AggregateFinal {
                                    plan_id: self.next_plan_id(),
                                    input: Box::new(input),
                                    group_by: group_items,
                                    agg_funcs,
                                    before_group_by_schema,

                                    stat_info: Some(stat_info),
                                    limit,
                                })
                            }

                            PhysicalPlan::Exchange(PhysicalExchange {
                                input: box PhysicalPlan::AggregatePartial(ref partial),
                                ..
                            }) => {
                                let before_group_by_schema = partial.input.output_schema()?;
                                let limit = agg.limit;

                                PhysicalPlan::AggregateFinal(AggregateFinal {
                                    plan_id: self.next_plan_id(),
                                    input: Box::new(input),
                                    group_by: group_items,
                                    agg_funcs,
                                    before_group_by_schema,

                                    stat_info: Some(stat_info),
                                    limit,
                                })
                            }

                            _ => {
                                return Err(ErrorCode::Internal(format!(
                                    "invalid input physical plan: {}",
                                    input.name(),
                                )));
                            }
                        }
                    }
                    AggregateMode::Initial => {
                        return Err(ErrorCode::Internal("Invalid aggregate mode: Initial"));
                    }
                };

                Ok(result)
            }
            RelOperator::Sort(sort) => Ok(PhysicalPlan::Sort(Sort {
                plan_id: self.next_plan_id(),
                input: Box::new(self.build(s_expr.child(0)?).await?),
                order_by: sort
                    .items
                    .iter()
                    .map(|v| SortDesc {
                        asc: v.asc,
                        nulls_first: v.nulls_first,
                        order_by: v.index,
                    })
                    .collect(),
                limit: sort.limit,

                stat_info: Some(stat_info),
            })),
            RelOperator::Limit(limit) => Ok(PhysicalPlan::Limit(Limit {
                plan_id: self.next_plan_id(),
                input: Box::new(self.build(s_expr.child(0)?).await?),
                limit: limit.limit,
                offset: limit.offset,

                stat_info: Some(stat_info),
            })),
            RelOperator::Exchange(exchange) => {
                let input = Box::new(self.build(s_expr.child(0)?).await?);
                let input_schema = input.output_schema()?;
                let mut keys = vec![];
                let kind = match exchange {
                    Exchange::Random => FragmentKind::Init,
                    Exchange::Hash(scalars) => {
                        for scalar in scalars {
                            let expr =
                                scalar
                                    .as_expr_with_col_index()?
                                    .project_column_ref(|index| {
                                        input_schema.index_of(&index.to_string()).unwrap()
                                    });
                            let (expr, _) = ConstantFolder::fold(
                                &expr,
                                self.ctx.get_function_context()?,
                                &BUILTIN_FUNCTIONS,
                            );
                            keys.push(expr.as_remote_expr());
                        }
                        FragmentKind::Normal
                    }
                    Exchange::Broadcast => FragmentKind::Expansive,
                    Exchange::Merge => FragmentKind::Merge,
                };
                Ok(PhysicalPlan::Exchange(PhysicalExchange {
                    input,
                    kind,
                    keys,
                }))
            }
            RelOperator::UnionAll(op) => {
                let left = self.build(s_expr.child(0)?).await?;
                let left_schema = left.output_schema()?;
                let pairs = op
                    .pairs
                    .iter()
                    .map(|(l, r)| (l.to_string(), r.to_string()))
                    .collect::<Vec<_>>();
                let fields = pairs
                    .iter()
                    .map(|(left, _)| Ok(left_schema.field_with_name(left)?.clone()))
                    .collect::<Result<Vec<_>>>()?;
                Ok(PhysicalPlan::UnionAll(UnionAll {
                    plan_id: self.next_plan_id(),
                    left: Box::new(left),
                    right: Box::new(self.build(s_expr.child(1)?).await?),
                    pairs,
                    schema: DataSchemaRefExt::create(fields),

                    stat_info: Some(stat_info),
                }))
            }
            RelOperator::RuntimeFilterSource(op) => {
                let left_side = Box::new(self.build(s_expr.child(0)?).await?);
                let left_schema = left_side.output_schema()?;
                let right_side = Box::new(self.build(s_expr.child(1)?).await?);
                let right_schema = right_side.output_schema()?;
                let mut left_runtime_filters = BTreeMap::new();
                let mut right_runtime_filters = BTreeMap::new();
                for (left, right) in op
                    .left_runtime_filters
                    .iter()
                    .zip(op.right_runtime_filters.iter())
                {
                    left_runtime_filters.insert(
                        left.0.clone(),
                        left.1
                            .as_expr_with_col_index()?
                            .project_column_ref(|index| {
                                left_schema.index_of(&index.to_string()).unwrap()
                            })
                            .as_remote_expr(),
                    );
                    right_runtime_filters.insert(
                        right.0.clone(),
                        right
                            .1
                            .as_expr_with_col_index()?
                            .project_column_ref(|index| {
                                right_schema.index_of(&index.to_string()).unwrap()
                            })
                            .as_remote_expr(),
                    );
                }
                Ok(PhysicalPlan::RuntimeFilterSource(RuntimeFilterSource {
                    plan_id: self.next_plan_id(),
                    left_side,
                    right_side,
                    left_runtime_filters,
                    right_runtime_filters,
                }))
            }
            _ => Err(ErrorCode::Internal(format!(
                "Unsupported physical plan: {:?}",
                s_expr.plan()
            ))),
        }
    }

    fn push_downs(
        &self,
        scan: &Scan,
        table_schema: &TableSchema,
        has_inner_column: bool,
    ) -> Result<PushDownInfo> {
        let metadata = self.metadata.read().clone();
        let projection = Self::build_projection(
            &metadata,
            table_schema,
            &scan.columns,
            has_inner_column,
            // for projection, we need to ignore read data from internal column,
            // or else in read_partition when search internal column from table schema will core.
            true,
        );

        let push_down_filter = scan
            .push_down_predicates
            .as_ref()
            .filter(|p| !p.is_empty())
            .map(|predicates| -> Result<RemoteExpr<String>> {
                let predicates: Result<Vec<Expr<String>>> = predicates
                    .iter()
                    .map(|p| p.as_expr_with_col_name())
                    .collect();

                let predicates = predicates?;
                let expr = predicates
                    .into_iter()
                    .reduce(|lhs, rhs| {
                        check_function(None, "and", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS).unwrap()
                    })
                    .unwrap();

                let expr = cast_expr_to_non_null_boolean(expr)?;

                let (expr, _) = ConstantFolder::fold(
                    &expr,
                    self.ctx.get_function_context()?,
                    &BUILTIN_FUNCTIONS,
                );
                Ok(expr.as_remote_expr())
            })
            .transpose()?;

        let prewhere_info = scan
            .prewhere
            .as_ref()
            .map(|prewhere| -> Result<PrewhereInfo> {
                let remain_columns = scan
                    .columns
                    .difference(&prewhere.prewhere_columns)
                    .copied()
                    .collect::<HashSet<usize>>();

                let output_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    &prewhere.output_columns,
                    has_inner_column,
                    false,
                );
                let prewhere_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    &prewhere.prewhere_columns,
                    has_inner_column,
                    false,
                );
                let remain_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    &remain_columns,
                    has_inner_column,
                    false,
                );

                let predicate = prewhere
                    .predicates
                    .iter()
                    .cloned()
                    .reduce(|lhs, rhs| {
                        ScalarExpr::AndExpr(AndExpr {
                            left: Box::new(lhs),
                            right: Box::new(rhs),
                        })
                    })
                    .expect("there should be at least one predicate in prewhere");
                let expr = cast_expr_to_non_null_boolean(predicate.as_expr_with_col_name()?)?;
                let (filter, _) = ConstantFolder::fold(
                    &expr,
                    self.ctx.get_function_context()?,
                    &BUILTIN_FUNCTIONS,
                );
                let filter = filter.as_remote_expr();

                Ok::<PrewhereInfo, ErrorCode>(PrewhereInfo {
                    output_columns,
                    prewhere_columns,
                    remain_columns,
                    filter,
                })
            })
            .transpose()?;

        let order_by = scan
            .order_by
            .clone()
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| {
                        let metadata = self.metadata.read();
                        let column = metadata.column(item.index);
                        let (name, data_type) = match column {
                            ColumnEntry::BaseTableColumn(BaseTableColumn {
                                column_name,
                                data_type,
                                ..
                            }) => (column_name.clone(), DataType::from(data_type)),
                            ColumnEntry::DerivedColumn(DerivedColumn {
                                alias, data_type, ..
                            }) => (alias.clone(), data_type.clone()),
                            ColumnEntry::InternalColumn(TableInternalColumn {
                                internal_column,
                                ..
                            }) => (
                                internal_column.column_name().to_owned(),
                                internal_column.data_type(),
                            ),
                        };

                        // sort item is already a column
                        let scalar = RemoteExpr::ColumnRef {
                            span: None,
                            id: name.clone(),
                            data_type,
                            display_name: name,
                        };

                        Ok((scalar, item.asc, item.nulls_first))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        Ok(PushDownInfo {
            projection: Some(projection),
            filter: push_down_filter,
            prewhere: prewhere_info,
            limit: scan.limit,
            order_by: order_by.unwrap_or_default(),
        })
    }

    fn build_plan_stat_info(&self, s_expr: &SExpr) -> Result<PlanStatsInfo> {
        let rel_expr = RelExpr::with_s_expr(s_expr);
        let prop = rel_expr.derive_relational_prop()?;

        Ok(PlanStatsInfo {
            estimated_rows: prop.cardinality,
        })
    }
}
