---
title: MIN_IF
---


## MIN_IF 

The suffix `_IF` can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```
MIN_IF(column, cond)
```

## Examples

:::tip
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
SELECT min(number) FROM numbers(10);
+-------------+
| min(number) |
+-------------+
|           0 |
+-------------+

SELECT min_if(number, number > 7) FROM numbers(10);
+------------------------------+
| min_if(number, (number > 7)) |
+------------------------------+
|                            8 |
+------------------------------+
```
