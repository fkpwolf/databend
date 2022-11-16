---
title: TAN
description: TAN(x) function
---

Returns the tangent of x, where x is given in radians.

## Syntax

```sql
TAN(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The angle, in radians. |

## Return Type

A Float64 data type value.

## Examples

```sql
SELECT TAN(PI());
+-------------------------------------+
| TAN(PI())                           |
+-------------------------------------+
| -0.00000000000000012246467991473532 |
+-------------------------------------+

SELECT TAN(PI()+1);
+--------------------+
| TAN((PI() + 1))    |
+--------------------+
| 1.5574077246549016 |
+--------------------+
```
