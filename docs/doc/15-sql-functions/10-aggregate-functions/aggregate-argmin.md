---
title: ARG_MIN
---

Calculates the `arg` value for a minimum `val` value. If there are several different values of `arg` for minimum values of `val`, returns the first of these values encountered.

## Syntax

```
ARG_MIN(arg, val)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| arg | Argument of [any data type that Databend supports](../../13-sql-reference/10-data-types/index.md)|
| val | Value of [any data type that Databend supports](../../13-sql-reference/10-data-types/index.md)|

## Return Type

`arg` value that corresponds to minimum `val` value.

 matches `arg` type.

## Examples

:::tip
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

Input table:

```sql
SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user ORDER BY salary ASC;
+----------+------+
| salary   | user |
+----------+------+
| 16661667 |    1 |
| 16665000 |    2 |
| 16668333 |    0 |
+----------+------+
```

```sql
SELECT arg_min(user, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user);
+-----------------------+
| arg_min(user, salary) |
+-----------------------+
|                     1 |
+-----------------------+

```
