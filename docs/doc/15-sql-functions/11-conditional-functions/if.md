---
title: IF
description: 'IF( <expr1>, <expr2>, <expr3> ) function'
---

If expr1 is TRUE, IF() returns expr2. Otherwise, it returns expr3.

## Syntax

```sql
IF( <expr1>, <expr2>, <expr3>)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr1>` | The condition for evaluation that can be true or false. |
| `<expr2>` | The expression to return if condition is met. |
| `<expr3>` | The expression to return if condition is not met. |

## Return Type

The return type is determined by expr2 and expr3, they must have the lowest common type.

## Examples

```sql
SELECT if(number=0, true, false) FROM numbers(1);
+-------------------------------+
| if((number = 0), true, false) |
+-------------------------------+
|                             1 |
+-------------------------------+
```

```sql
SELECT if(number > 5, number*5, number+5 ) FROM numbers(10);
+----------------------------------------------+
| if((number > 5), (number * 5), (number + 5)) |
+----------------------------------------------+
|                                            5 |
|                                            6 |
|                                            7 |
|                                            8 |
|                                            9 |
|                                           10 |
|                                           30 |
|                                           35 |
|                                           40 |
|                                           45 |
+----------------------------------------------+
```
