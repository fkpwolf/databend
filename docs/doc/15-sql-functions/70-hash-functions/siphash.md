---
title: SIPHASH
title_includes: SIPHASH, SIPHASH64
---

Produces a 64-bit [SipHash](https://131002.net/siphash) hash value.

## Syntax

```sql
siphash(expression)
siphash64(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression. <br /> This may be a column name, the result of another function, or a math operation.

## Return Type

A UInt64 data type hash value.


## Examples

```sql
SELECT SIPHASH('1234567890');
+-----------------------+
| SIPHASH('1234567890') |
+-----------------------+
|  18110648197875983073 |
+-----------------------+

SELECT SIPHASH(1);
+---------------------+
| SIPHASH(1)          |
+---------------------+
| 4952851536318644461 |
+---------------------+

SELECT SIPHASH(1.2);
+---------------------+
| SIPHASH(1.2)        |
+---------------------+
| 2854037594257667269 |
+---------------------+

SELECT SIPHASH(number) FROM numbers(2);
+----------------------+
| SIPHASH(number)      |
+----------------------+
| 13646096770106105413 |
|  2206609067086327257 |
+----------------------+

SELECT SIPHASH64('1234567890');
+-------------------------+
| SIPHASH64('1234567890') |
+-------------------------+
|    18110648197875983073 |
+-------------------------+

```
