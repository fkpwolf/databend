---
title: COUNT_DISTINCT
title_includes: uniq
---

Aggregate function.

The count(distinct ...) function calculates the uniq value of a set of values.

:::caution
 NULL values are not counted.
:::

## Syntax

```
COUNT(distinct arguments ...)
UNIQ(arguments)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression, size of the arguments is [1, 32] |

## Return Type

UInt64

## Examples

```sql
SELECT count(distinct number % 3) FROM numbers(1000);
+------------------------------+
| count(distinct (number % 3)) |
+------------------------------+
|                            3 |
+------------------------------+

 SELECT uniq(number % 3, number) FROM numbers(1000);
+----------------------------+
| uniq((number % 3), number) |
+----------------------------+
|                       1000 |
+----------------------------+


 SELECT uniq(number % 3, number) = count(distinct number %3, number)  FROM numbers(1000);
+---------------------------------------------------------------------+
| (uniq((number % 3), number) = count(distinct (number % 3), number)) |
+---------------------------------------------------------------------+
|                                                                true |
+---------------------------------------------------------------------+
```

