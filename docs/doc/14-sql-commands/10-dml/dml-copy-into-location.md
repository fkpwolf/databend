---
title: 'COPY INTO <location>'
sidebar_label: 'COPY INTO <location>'
description:
  'Unload Data using COPY INTO <location>'
---

`COPY` loads data into Databend or unloads data from Databend.

This command unloads data from a table (or query) into one or more files in one of the following locations:

* Named internal stage: The files can be downloaded from the stage using the GET command.
* Named external stage: An external location (including Amazon S3).
* External location: An object storage system (including Amazon S3).

See Also: [COPY INTO table](dml-copy-into-table.md)

## Syntax

```sql
COPY INTO { internalStage | externalStage | externalLocation }
FROM { [<database_name>.]<table_name> | ( <query> ) }
[ FILE_FORMAT = ( { TYPE = { CSV | JSON | NDJSON | PARQUET } [ formatTypeOptions ] } ) ]
[ copyOptions ]
[ VALIDATION_MODE = RETURN_ROWS ]
```

Where:

### internalStage

```sql
internalStage ::= @<internal_stage_name>[/<path>]
```

### externalStage

```sql
externalStage ::= @<external_stage_name>[/<path>]
```

### externalLocation (for Amazon S3)

```
externalLocation (for Amazon S3) ::=
  's3://<bucket>[/<path>]'
  [ { CONNECTION = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' } } ) } ]
```

| Parameter  | Description | Required |
| ----------- | ----------- | --- |
| `s3://<bucket>/[<path>]`  | Files are in the specified external location (S3-like bucket) | YES |
| `[ { CONNECTION = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' } } ) } ]' ]`  | The credentials for connecting to AWS and accessing the private/protected S3 bucket where the files to load are staged. |  Optional |
| `[ ENDPOINT_URL = '<endpoint_url>' ]`  | S3-compatible endpoint URL like MinIO, default is `https://s3.amazonaws.com` |  Optional |


### FILE_FORMAT

See [Input & Output File Formats](../../13-sql-reference/75-file-format-options.md).

### copyOptions
```
copyOptions ::=
  [ SINGLE = TRUE | FALSE ]
  [ MAX_FILE_SIZE = <num> ]
```

| Parameter  | Description | Required |
| ----------- | ----------- | --- |
| `SINGLE` | When TRUE, the command unloads data into one single file. Default: FALSE. | Optional |
| `MAX_FILE_SIZE` | The maximum size (in bytes) of each file to be created.<br />Effective when `SINGLE` is FALSE. Default: 67108864 (64 MB). | Optional |

## Examples

The following examples unload data into an internal stage:

```sql
-- Create a table
CREATE TABLE test_table (
     id INTEGER,
     name VARCHAR,
     age INT
);

-- Insert data into the table
INSERT INTO test_table (id,name,age) VALUES(1,'2',3), (4, '5', 6);

-- Create an internal stage
CREATE STAGE s2;

-- Unload the data in the table into a CSV file on the stage
COPY INTO @s2 FROM test_table FILE_FORMAT = (TYPE = CSV);

-- Unload the data from a query into a parquet file on the stage
COPY INTO @s2 FROM (SELECT name, age, id FROM test_table LIMIT 100) FILE_FORMAT = (TYPE = 'PARQUET');
```