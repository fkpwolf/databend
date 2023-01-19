---
title: Roadmap 2023
sidebar_label: Roadmap
description:
  Roadmap 2023
---


:::tip
This is Databend Roadmap in 2023 :rocket:, sync from the [#9448](https://github.com/datafuselabs/databend/issues/9448)
:::

After a full year of research and development in 2022, the functionality and stability of Databend were significantly enhanced, and several users began using it in production. Databend has helped them greatly **reduce** costs and operational complexity issues.

This is Databend Roadmap in 2023 (discussion).

See also:
* Roadmap 2022: [#3706](https://github.com/datafuselabs/databend/issues/3706)
* Roadmap 2021: [#746](https://github.com/datafuselabs/databend/issues/746)

# Main tasks

## Features

| Task                                                                               | Status      | Comments |
|------------------------------------------------------------------------------------|-------------|----------|
| Update                                                                             | IN PROGRESS |          |
| Merge                                                                              | PLAN        |          |
| Alter table                                                                        | IN PROGRESS |          |
| Window function                                                                    | PLAN        |          |
| Lambda function and high-order functions                                           | PLAN        |          |
| TimestampTz data type                                                              | PLAN        |          |
| Decimal data type                                                                  | PLAN        |          |
| Materialized view                                                                  | PLAN        |          |
| [Support SET_VAR hints#8833](https://github.com/datafuselabs/databend/issues/8833) | PLAN        |          |
| Parquet reader                                                                     | PLAN        |          |
| Distributed COPY                                                                   | PLAN        |          |
| JSON indexing                                                                      | PLAN        |          |
| DataFrame                                                                          | PLAN        |          |
| Data Sharing(community version)                                                    | IN PROGRESS |          |
| Concurrent query enhance                                                   | PLAN |          |

## Improvements

| Task                                                                      | Status      | Comments |
|---------------------------------------------------------------------------|-------------|----------|
| [New expression#9411](https://github.com/datafuselabs/databend/pull/9411) | IN PROGRESS |          |
| Error message                                                             | PLAN        |          |

## Planner

| Task                                                                                         | Status      | Comments             |
|----------------------------------------------------------------------------------------------|-------------|----------------------|
| Scalar expression normalization                                                              | PLAN        |                      |
| Column constraint framework                                                                  | PLAN        |                      |
| [Functional dependency framework#7438](https://github.com/datafuselabs/databend/issues/7438) | PLAN        |                      |
| Join reorder                                                                                 | IN PROGRESS |                      |
| CBO for distributed plan                                                                     | PLAN        |                      |
| Support TPC-DS                                                                               | PLAN        |                      |
| Support optimization tracing                                                                 | PLAN        | Easy to debug/study. |

## Cache

| Task                | Status  | Comments |
|---------------------|---------|----------|
| Unified cache layer | IN PROGRESS |          |
| Meta data cache     | IN PROGRESS |          |
| Index data cache    | IN PROGRESS |          |
| Block data cache    | PLAN    |          |

## Data Storage

| Task                            | Status | Comments                               |
|---------------------------------|--------|----------------------------------------|
| Fuse engine re-clustering       | PLAN   |                                        |
| Fuse engine orphan data cleanup | PLAN   |                                        |
| Fuse engine segment tree        | PLAN   | Support large dataset(PB) in one table |

## LakeHouse

| Task                               | Status      | Comments |
|------------------------------------|-------------|----------|
| Apache Hive                        | IN PROGRESS |          |
| Apache Iceberg                     | IN PROGRESS |          |
| Querying external storage(Parquet) | IN PROGRESS |          |

## Distributed Query Execution

| Task                 | Status      | Comments |
|----------------------|-------------|----------|
| Visualized profiling | IN PROGRESS |          |
| Aggregation spilling | IN PROGRESS |          |

## Resource Quota

| Task                                     | Status      | Comments |
|------------------------------------------|-------------|----------|
| Session-level quota control (CPU/Memory) | IN PROGRESS |          |
| User-level quota control (CPU/Memory)    | PLAN        |          |


## Integrations

| Task                                      | Status      | Comments |
|-------------------------------------------|-------------|----------|
| Dbt integration                           | IN PROGRESS |          |
| Airbyte integration                       | IN PROGRESS |          |
| Datadog Vector integrate with Rust-driver | IN PROGRESS |          |
| Datax integrate with Java-driver          | IN PROGRESS |          |
| CDC with Flink                            | PLAN        |          |
| CDC with Kafka                            | PLAN        |          |


## Meta

| Task        | Status      | Comments |
|-------------|-------------|----------|
| Jepsen test | IN PROGRESS |          |

## Testing

| Task          | Status      | Comments                          |
|---------------|-------------|-----------------------------------|
| SQLlogic Test | IN PROGRESS | Supports more test cases          |
| SQLancer Test | IN PROGRESS | Supports more type and more cases |
| Fuzzer Test   | PLAN        |                                   |

# Releases
- [x] [v0.8 #4591](https://github.com/datafuselabs/databend/issues/4591)
- [x] [v0.7 #3428](https://github.com/datafuselabs/databend/issues/2328)
- [x] [v0.6 #2525](https://github.com/datafuselabs/databend/issues/2525)
- [x] [v0.5 #2257](https://github.com/datafuselabs/databend/issues/2257)

