---
title: REMOVE STAGE FILES
sidebar_label: REMOVE STAGE FILES
---

Removes files from a stage.

## Syntax

```sql
REMOVE { userStage | internalStage | externalStage } [ PATTERN = '<regex_pattern>' ]
```
Where:

### internalStage

```sql
internalStage ::= @<internal_stage_name>[/<file>]
```

### externalStage

```sql
externalStage ::= @<external_stage_name>[/<file>]
```

### PATTERN = 'regex_pattern'

A regular expression pattern string, enclosed in single quotes, filters files to remove by their filename.

## Examples

This command removes all the files with a name matching the pattern *'ontime.*'* from the stage named *playground*:

```sql
REMOVE @playground PATTERN = 'ontime.*'
```
