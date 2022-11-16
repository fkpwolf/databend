---
title: Databend on Scaleway OS Performance
sidebar_label: On Scaleway OS Performance
description:
  Sub-second analytics on Scaleway + OS experience.
---

:::tip
* Hardware: General(GP1-XL, nl-ams), 48 vCPUs
* Storage: Scaleway Object Storage (nl-ams)
* Dataset: [ontime](https://transtats.bts.gov/PREZIP/), 60.8 GB Raw CSV Data, 195662214 records
* Databend: [v0.6.100-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.6.100-nightly)
* No Local Caching(Data Caching/Query Result Caching)
* Databend is automatic performance optimization with **no manual tuning** required.
* [Analyzing OnTime Datasets with Databend on Scaleway and OS](../21-use-cases/01-analyze-ontime-with-databend-on-ec2-and-s3.md)
:::

## Q1 (0.673 sec., 90.69 million rows/sec., 272.07 MB/sec.)

```sql
SELECT
    DayOfWeek,
    count(*) AS c
FROM ontime
WHERE (Year >= 2000) AND (Year <= 2008)
GROUP BY DayOfWeek
ORDER BY c DESC
```

```text
+-----------+---------+
| DayOfWeek | c       |
+-----------+---------+
|         5 | 8732422 |
|         1 | 8730614 |
|         4 | 8710843 |
|         3 | 8685626 |
|         2 | 8639632 |
|         7 | 8274367 |
|         6 | 7514194 |
+-----------+---------+
7 rows in set (1.34 sec)
Read 61000000 rows, 183 MB in 0.673 sec., 90.69 million rows/sec., 272.07 MB/sec.
```

```text
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: DayOfWeek:UInt8, count() as c:UInt64                                                                                                                                                                                                           │
│   Sort: count():UInt64                                                                                                                                                                                                                                     │
│     AggregatorFinal: groupBy=[[DayOfWeek]], aggr=[[count()]]                                                                                                                                                                                               │
│       AggregatorPartial: groupBy=[[DayOfWeek]], aggr=[[count()]]                                                                                                                                                                                           │
│         Filter: ((Year >= 2000) and (Year <= 2008))                                                                                                                                                                                                        │
│           ReadDataSource: scan schema: [Year:UInt16, DayOfWeek:UInt8], statistics: [read_rows: 61000000, read_bytes: 183000000, partitions_scanned: 61, partitions_total: 203], push_downs: [projections: [0, 4], filters: [((Year >= 2000) AND (Year <= 2008))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q2 (0.753 sec., 81.05 million rows/sec., 567.34 MB/sec.)

```sql
SELECT
    DayOfWeek,
    count(*) AS c
FROM ontime
WHERE (DepDelay > 10) AND (Year >= 2000) AND (Year <= 2008)
GROUP BY DayOfWeek
ORDER BY c DESC
```

```text
+-----------+---------+
| DayOfWeek | c       |
+-----------+---------+
|         5 | 2175733 |
|         4 | 2012848 |
|         1 | 1898879 |
|         7 | 1880896 |
|         3 | 1757508 |
|         2 | 1665303 |
|         6 | 1510894 |
+-----------+---------+
7 rows in set (1.51 sec)
Read 61000000 rows, 427 MB in 0.753 sec., 81.05 million rows/sec., 567.34 MB/sec.
```

```text
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: DayOfWeek:UInt8, count() as c:UInt64                                                                                                                                                                                                           │
│   Sort: count():UInt64                                                                                                                                                                                                                                     │
│     AggregatorFinal: groupBy=[[DayOfWeek]], aggr=[[count()]]                                                                                                                                                                                               │
│       AggregatorPartial: groupBy=[[DayOfWeek]], aggr=[[count()]]                                                                                                                                                                                           │
│         Filter: (((DepDelay > 10) and (Year >= 2000)) and (Year <= 2008))                                                                                                                                                                                  │
│           ReadDataSource: scan schema: [Year:UInt16, DayOfWeek:UInt8, DepDelay:Int32], statistics: [read_rows: 61000000, read_bytes: 427000000, partitions_scanned: 61, partitions_total: 203], push_downs: [projections: [0, 4, 31], filters: [(((DepDelay > 10) AND (Year >= 2000)) AND (Year <= 2008))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q3 (0.864 sec., 70.61 million rows/sec., 1.2 GB/sec.)

```sql
SELECT
    Origin,
    count(*) AS c
FROM ontime
WHERE (DepDelay > 10) AND (Year >= 2000) AND (Year <= 2008)
GROUP BY Origin
ORDER BY c DESC
LIMIT 10
```

```text
+--------+--------+
| Origin | c      |
+--------+--------+
| ORD    | 860911 |
| ATL    | 831822 |
| DFW    | 614403 |
| LAX    | 402671 |
| PHX    | 400475 |
| LAS    | 362026 |
| DEN    | 352893 |
| EWR    | 302267 |
| DTW    | 296832 |
| IAH    | 290729 |
+--------+--------+
10 rows in set (1.56 sec)
Read 61000000 rows, 1.04 GB in 0.864 sec., 70.61 million rows/sec., 1.2 GB/sec.
```

```text
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Limit: 10                                                                                                                                                                                                                                                  │
│   Projection: Origin:String, count() as c:UInt64                                                                                                                                                                                                           │
│     Sort: count():UInt64                                                                                                                                                                                                                                   │
│       AggregatorFinal: groupBy=[[Origin]], aggr=[[count()]]                                                                                                                                                                                                │
│         AggregatorPartial: groupBy=[[Origin]], aggr=[[count()]]                                                                                                                                                                                            │
│           Filter: (((DepDelay > 10) and (Year >= 2000)) and (Year <= 2008))                                                                                                                                                                                │
│             ReadDataSource: scan schema: [Year:UInt16, Origin:String, DepDelay:Int32], statistics: [read_rows: 61000000, read_bytes: 1037000488, partitions_scanned: 61, partitions_total: 203], push_downs: [projections: [0, 14, 31], filters: [(((DepDelay > 10) AND (Year >= 2000)) AND (Year <= 2008))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q4 (0.369 sec., 21.69 million rows/sec., 346.98 MB/sec.)

```sql
SELECT
    IATA_CODE_Reporting_Airline AS Carrier,
    count()
FROM ontime
WHERE (DepDelay > 10) AND (Year = 2007)
GROUP BY Carrier
ORDER BY count() DESC
```

```text
+---------+---------+
| Carrier | count() |
+---------+---------+
| WN      |  296451 |
| AA      |  179769 |
| MQ      |  152293 |
| OO      |  147019 |
| US      |  140199 |
| UA      |  135061 |
| XE      |  108571 |
| EV      |  104055 |
| NW      |  102206 |
| DL      |   98427 |
| CO      |   81039 |
| YV      |   79553 |
| FL      |   64583 |
| OH      |   60532 |
| AS      |   54326 |
| B6      |   53716 |
| 9E      |   48578 |
| F9      |   24100 |
| AQ      |    6764 |
| HA      |    4059 |
+---------+---------+
20 rows in set (1.10 sec)
Read 8000000 rows, 128 MB in 0.369 sec., 21.69 million rows/sec., 346.98 MB/sec.
```

```text
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: IATA_CODE_Reporting_Airline as Carrier:String, count():UInt64                                                                                                                                                                                  │
│   Sort: count():UInt64                                                                                                                                                                                                                                     │
│     AggregatorFinal: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[count()]]                                                                                                                                                                             │
│       AggregatorPartial: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[count()]]                                                                                                                                                                         │
│         Filter: ((DepDelay > 10) and (Year = 2007))                                                                                                                                                                                                        │
│           ReadDataSource: scan schema: [Year:UInt16, IATA_CODE_Reporting_Airline:String, DepDelay:Int32], statistics: [read_rows: 8000000, read_bytes: 128000064, partitions_scanned: 8, partitions_total: 203], push_downs: [projections: [0, 8, 31], filters: [((DepDelay > 10) AND (Year = 2007))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q5 (0.371 sec., 21.54 million rows/sec., 344.67 MB/sec.)

```sql
SELECT
    IATA_CODE_Reporting_Airline AS Carrier,
    avg(CAST(DepDelay > 10, Int8)) * 1000 AS c3
FROM ontime
WHERE Year = 2007
GROUP BY Carrier
ORDER BY c3 DESC
```

```text
+---------+--------------------+
| Carrier | c3                 |
+---------+--------------------+
| EV      | 363.53123668047823 |
| AS      |  339.1453631738303 |
| US      |  288.8039271022377 |
| AA      |  283.6112877194699 |
| MQ      |  281.7663100792978 |
| B6      |  280.5745625489684 |
| UA      | 275.63356884257615 |
| YV      | 270.25567158804466 |
| OH      |  256.4567516268981 |
| WN      | 253.62165713752844 |
| CO      | 250.77750030171651 |
| XE      | 249.71881878589517 |
| NW      | 246.56113247419944 |
| F9      | 246.52209492635023 |
| OO      | 245.90051515354253 |
| FL      |  245.4143692596491 |
| DL      | 206.82764258051773 |
| 9E      | 187.66780889391967 |
| AQ      |  145.9016393442623 |
| HA      |  72.25634178905207 |
+---------+--------------------+
20 rows in set (1.07 sec)
Read 8000000 rows, 128 MB in 0.371 sec., 21.54 million rows/sec., 344.67 MB/sec.
```

```text
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: IATA_CODE_Reporting_Airline as Carrier:String, (avg(cast((DepDelay > 10) as Int8)) * 1000) as c3:Float64                                                                                                                                       │
│   Sort: (avg(cast((DepDelay > 10) as Int8)) * 1000):Float64                                                                                                                                                                                                │
│     Expression: IATA_CODE_Reporting_Airline:String, (avg(cast((DepDelay > 10) as Int8)) * 1000):Float64 (Before OrderBy)                                                                                                                                   │
│       AggregatorFinal: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(cast((DepDelay > 10) as Int8))]]                                                                                                                                                │
│         AggregatorPartial: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(cast((DepDelay > 10) as Int8))]]                                                                                                                                            │
│           Expression: IATA_CODE_Reporting_Airline:String, cast((DepDelay > 10) as Int8):Int8 (Before GroupBy)                                                                                                                                              │
│             Filter: (Year = 2007)                                                                                                                                                                                                                          │
│               ReadDataSource: scan schema: [Year:UInt16, IATA_CODE_Reporting_Airline:String, DepDelay:Int32], statistics: [read_rows: 8000000, read_bytes: 128000064, partitions_scanned: 8, partitions_total: 203], push_downs: [projections: [0, 8, 31], filters: [(Year = 2007)]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q6 (0.990 sec., 61.59 million rows/sec., 985.5 MB/sec.)

```sql
SELECT
    IATA_CODE_Reporting_Airline AS Carrier,
    avg(CAST(DepDelay > 10, Int8)) * 1000 AS c3
FROM ontime
WHERE (Year >= 2000) AND (Year <= 2008)
GROUP BY Carrier
ORDER BY c3 DESC
```

```text
+---------+--------------------+
| Carrier | c3                 |
+---------+--------------------+
| AS      | 293.05649076611434 |
| EV      |  282.0709981074399 |
| YV      |  270.3897636688929 |
| B6      | 257.40594891667007 |
| FL      | 249.28742951361826 |
| XE      | 246.59005902424192 |
| MQ      |  245.3695989400477 |
| WN      | 233.38127235928863 |
| DH      | 227.11013827345042 |
| F9      | 226.08455653226812 |
| UA      | 224.42824657703645 |
| OH      | 215.52882835147614 |
| AA      | 211.97122176454556 |
| US      | 206.60330294168244 |
| HP      | 205.31690167066455 |
| OO      |  202.4243177198239 |
| NW      |  191.7393936377831 |
| TW      |  188.6912623180138 |
| DL      | 187.84162871590732 |
| CO      | 187.71301306878976 |
| 9E      |  181.6396991511518 |
| RU      | 181.46244295416398 |
| TZ      |  176.8928125899626 |
| AQ      | 145.65911608293766 |
| HA      |  79.38672451825789 |
+---------+--------------------+
25 rows in set (1.63 sec)
Read 61000000 rows, 976 MB in 0.990 sec., 61.59 million rows/sec., 985.5 MB/sec.
```

```text
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: IATA_CODE_Reporting_Airline as Carrier:String, (avg(cast((DepDelay > 10) as Int8)) * 1000) as c3:Float64                                                                                                                                       │
│   Sort: (avg(cast((DepDelay > 10) as Int8)) * 1000):Float64                                                                                                                                                                                                │
│     Expression: IATA_CODE_Reporting_Airline:String, (avg(cast((DepDelay > 10) as Int8)) * 1000):Float64 (Before OrderBy)                                                                                                                                   │
│       AggregatorFinal: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(cast((DepDelay > 10) as Int8))]]                                                                                                                                                │
│         AggregatorPartial: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(cast((DepDelay > 10) as Int8))]]                                                                                                                                            │
│           Expression: IATA_CODE_Reporting_Airline:String, cast((DepDelay > 10) as Int8):Int8 (Before GroupBy)                                                                                                                                              │
│             Filter: ((Year >= 2000) and (Year <= 2008))                                                                                                                                                                                                    │
│               ReadDataSource: scan schema: [Year:UInt16, IATA_CODE_Reporting_Airline:String, DepDelay:Int32], statistics: [read_rows: 61000000, read_bytes: 976000488, partitions_scanned: 61, partitions_total: 203], push_downs: [projections: [0, 8, 31], filters: [((Year >= 2000) AND (Year <= 2008))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q7 (1.044 sec., 58.45 million rows/sec., 935.18 MB/sec.)

```sql
SELECT
    IATA_CODE_Reporting_Airline AS Carrier,
    avg(DepDelay) * 1000 AS c3
FROM ontime
WHERE (Year >= 2000) AND (Year <= 2008)
GROUP BY Carrier
```

```text
+---------+--------------------+
| Carrier | c3                 |
+---------+--------------------+
| NW      | 11717.623092632819 |
| 9E      | 13091.087573576122 |
| AQ      |  7323.278123603293 |
| WN      | 10484.932610056378 |
| UA      | 14594.243159716054 |
| DL      | 10943.456441165357 |
| DH      | 15311.949983190174 |
| TZ      | 12618.760195758565 |
| B6      | 16789.739456036365 |
| HA      |  6851.555976883671 |
| TW      | 10842.722114986364 |
| FL      | 15192.451732538268 |
| OH      | 12655.103820799075 |
| F9      | 11232.889558936127 |
| MQ      | 14125.201554023559 |
| CO      | 12671.595978518368 |
| RU      | 12556.249210602802 |
| EV      | 16374.703330010156 |
| XE      | 17092.548853057146 |
| OO      | 11600.594852741107 |
| AA      |  13508.78515494305 |
| HP      | 11625.682112859839 |
| US      |   11868.7097884053 |
| YV      |  17971.53933699898 |
| AS      | 14735.545887755581 |
+---------+--------------------+
25 rows in set (1.78 sec)
Read 61000000 rows, 976 MB in 1.044 sec., 58.45 million rows/sec., 935.18 MB/sec.
```

```text
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: IATA_CODE_Reporting_Airline as Carrier:String, (avg(DepDelay) * 1000) as c3:Float64                                                                                                                                                            │
│   Expression: IATA_CODE_Reporting_Airline:String, (avg(DepDelay) * 1000):Float64 (Before Projection)                                                                                                                                                       │
│     AggregatorFinal: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(DepDelay)]]                                                                                                                                                                       │
│       AggregatorPartial: groupBy=[[IATA_CODE_Reporting_Airline]], aggr=[[avg(DepDelay)]]                                                                                                                                                                   │
│         Filter: ((Year >= 2000) and (Year <= 2008))                                                                                                                                                                                                        │
│           ReadDataSource: scan schema: [Year:UInt16, IATA_CODE_Reporting_Airline:String, DepDelay:Int32], statistics: [read_rows: 61000000, read_bytes: 976000488, partitions_scanned: 61, partitions_total: 203], push_downs: [projections: [0, 8, 31], filters: [((Year >= 2000) AND (Year <= 2008))]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q8 (1.484 sec., 136.59 million rows/sec., 819.54 MB/sec.)

```sql
SELECT
    Year,
    avg(DepDelay)
FROM ontime
GROUP BY Year
```

```text
+------+--------------------+
| Year | avg(DepDelay)      |
+------+--------------------+
| 1987 |  8.600789281505321 |
| 1988 |  7.345867511864449 |
| 1989 |   8.81845473300008 |
| 1990 |  7.966702606180775 |
| 1991 |  6.940411174086677 |
| 1992 |  6.687364706154975 |
| 1993 |  7.207721091071671 |
| 1994 |  7.758752042452116 |
| 1995 |  9.328649903752932 |
| 1996 |  11.14468468976826 |
| 1997 |  9.919225483813925 |
| 1998 | 10.884314711941435 |
| 1999 | 11.567390524113748 |
| 2000 | 13.456897681824556 |
| 2001 | 10.895474364001354 |
| 2002 |   9.97856700710386 |
| 2003 |  9.778465263372038 |
| 2004 | 11.936799840656898 |
| 2005 |  12.60167890747495 |
| 2006 | 14.237297887039372 |
| 2007 | 15.431738868356579 |
| 2008 | 14.654588068064287 |
| 2009 | 13.168984006133062 |
| 2010 | 13.202976628175891 |
| 2011 | 13.496191548097778 |
| 2012 | 13.155971481255131 |
| 2013 | 14.901210490900201 |
| 2014 | 15.513697266113969 |
| 2015 | 14.638336410280733 |
| 2016 | 14.643883269504837 |
| 2017 |  15.70225324299191 |
| 2018 |  16.16188254545747 |
| 2019 | 16.983263489524507 |
| 2020 | 10.624498278073712 |
| 2021 | 15.289615417399649 |
+------+--------------------+
35 rows in set (1.95 sec)
Read 202687655 rows, 1.22 GB in 1.484 sec., 136.59 million rows/sec., 819.54 MB/sec.
```

```text
┌─explain──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: Year:UInt16, avg(DepDelay):Float64                                                                                                                                                                   │
│   AggregatorFinal: groupBy=[[Year]], aggr=[[avg(DepDelay)]]                                                                                                                                                      │
│     AggregatorPartial: groupBy=[[Year]], aggr=[[avg(DepDelay)]]                                                                                                                                                  │
│       ReadDataSource: scan schema: [Year:UInt16, DepDelay:Int32], statistics: [read_rows: 202687655, read_bytes: 1216125930, partitions_scanned: 203, partitions_total: 203], push_downs: [projections: [0, 31]] │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q9 (1.112 sec., 182.22 million rows/sec., 364.43 MB/sec.)
```sql
SELECT
    Year,
    count(*) AS c1
FROM ontime
GROUP BY Year
```

```text
+------+---------+
| Year | c1      |
+------+---------+
| 1987 | 1311826 |
| 1988 | 5202096 |
| 1989 | 5041200 |
| 1990 | 5270893 |
| 1991 | 5076925 |
| 1992 | 5092157 |
| 1993 | 5070501 |
| 1994 | 5180048 |
| 1995 | 5327435 |
| 1996 | 5351983 |
| 1997 | 5411843 |
| 1998 | 5384721 |
| 1999 | 5527884 |
| 2000 | 5683047 |
| 2001 | 5967780 |
| 2002 | 5271359 |
| 2003 | 6488540 |
| 2004 | 7129270 |
| 2005 | 7140596 |
| 2006 | 7141922 |
| 2007 | 7455458 |
| 2008 | 7009726 |
| 2009 | 6450285 |
| 2010 | 6450117 |
| 2011 | 6085281 |
| 2012 | 6096762 |
| 2013 | 6369482 |
| 2014 | 5819811 |
| 2015 | 5819079 |
| 2016 | 5617658 |
| 2017 | 5674621 |
| 2018 | 7213446 |
| 2019 | 7422037 |
| 2020 | 4688354 |
| 2021 | 5443512 |
+------+---------+
35 rows in set (1.85 sec)
Read 202687655 rows, 405.38 MB in 1.112 sec., 182.22 million rows/sec., 364.43 MB/sec.
```

```text
┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: Year:UInt16, count() as c1:UInt64                                                                                                                                               │
│   AggregatorFinal: groupBy=[[Year]], aggr=[[count()]]                                                                                                                                       │
│     AggregatorPartial: groupBy=[[Year]], aggr=[[count()]]                                                                                                                                   │
│       ReadDataSource: scan schema: [Year:UInt16], statistics: [read_rows: 202687655, read_bytes: 405375310, partitions_scanned: 203, partitions_total: 203], push_downs: [projections: [0]] │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q10 (1.463 sec., 138.52 million rows/sec., 969.61 MB/sec.)

```sql
SELECT avg(cnt)
FROM
(
    SELECT
        Year,
        Month,
        count(*) AS cnt
    FROM ontime
    WHERE DepDel15 = 1
    GROUP BY
        Year,
        Month
) AS a
```

```text
+-------------------+
| avg(cnt)          |
+-------------------+
| 81342.93170731707 |
+-------------------+
1 row in set (2.12 sec)
Read 202687655 rows, 1.42 GB in 1.463 sec., 138.52 million rows/sec., 969.61 MB/sec.
```

```text
┌─explain────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: avg(cnt):Float64                                                                                                                                                                                                                               │
│   AggregatorFinal: groupBy=[[]], aggr=[[avg(cnt)]]                                                                                                                                                                                                         │
│     AggregatorPartial: groupBy=[[]], aggr=[[avg(cnt)]]                                                                                                                                                                                                     │
│       Projection: Year:UInt16, Month:UInt8, count() as cnt:UInt64                                                                                                                                                                                          │
│         AggregatorFinal: groupBy=[[Year, Month]], aggr=[[count()]]                                                                                                                                                                                         │
│           AggregatorPartial: groupBy=[[Year, Month]], aggr=[[count()]]                                                                                                                                                                                     │
│             Filter: (DepDel15 = 1)                                                                                                                                                                                                                         │
│               ReadDataSource: scan schema: [Year:UInt16, Month:UInt8, DepDel15:Int32], statistics: [read_rows: 202687655, read_bytes: 1418813585, partitions_scanned: 203, partitions_total: 203], push_downs: [projections: [0, 2, 33], filters: [(DepDel15 = 1)]] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q11 (1.739 sec., 116.57 million rows/sec., 349.72 MB/sec.)

```sql
SELECT avg(c1)
FROM
(
    SELECT
        Year,
        Month,
        count(*) AS c1
    FROM ontime
    GROUP BY
        Year,
        Month
) AS a
```

```text
+-------------------+
| avg(c1)           |
+-------------------+
| 494360.1341463415 |
+-------------------+
1 row in set (2.54 sec)
Read 202687655 rows, 608.06 MB in 1.739 sec., 116.57 million rows/sec., 349.72 MB/sec.
```

```text
┌─explain───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: avg(c1):Float64                                                                                                                                                                                       │
│   AggregatorFinal: groupBy=[[]], aggr=[[avg(c1)]]                                                                                                                                                                 │
│     AggregatorPartial: groupBy=[[]], aggr=[[avg(c1)]]                                                                                                                                                             │
│       Projection: Year:UInt16, Month:UInt8, count() as c1:UInt64                                                                                                                                                  │
│         AggregatorFinal: groupBy=[[Year, Month]], aggr=[[count()]]                                                                                                                                                │
│           AggregatorPartial: groupBy=[[Year, Month]], aggr=[[count()]]                                                                                                                                            │
│             ReadDataSource: scan schema: [Year:UInt16, Month:UInt8], statistics: [read_rows: 202687655, read_bytes: 608062965, partitions_scanned: 203, partitions_total: 203], push_downs: [projections: [0, 2]] │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q12 (7.410 sec., 27.35 million rows/sec., 1.16 GB/sec.)

```sql
SELECT
    OriginCityName,
    DestCityName,
    count(*) AS c
FROM ontime
GROUP BY
    OriginCityName,
    DestCityName
ORDER BY c DESC
LIMIT 10
```

```text
+-------------------+-------------------+--------+
| OriginCityName    | DestCityName      | c      |
+-------------------+-------------------+--------+
| San Francisco, CA | Los Angeles, CA   | 518850 |
| Los Angeles, CA   | San Francisco, CA | 515980 |
| New York, NY      | Chicago, IL       | 457899 |
| Chicago, IL       | New York, NY      | 450557 |
| Chicago, IL       | Minneapolis, MN   | 439491 |
| Minneapolis, MN   | Chicago, IL       | 435311 |
| Los Angeles, CA   | Las Vegas, NV     | 431245 |
| Las Vegas, NV     | Los Angeles, CA   | 425188 |
| New York, NY      | Boston, MA        | 421854 |
| Boston, MA        | New York, NY      | 418572 |
+-------------------+-------------------+--------+
10 rows in set (8.22 sec)
Read 202687655 rows, 8.58 GB in 7.410 sec., 27.35 million rows/sec., 1.16 GB/sec.
```

```text
┌─explain──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Limit: 10                                                                                                                                                                                                                            │
│   Projection: OriginCityName:String, DestCityName:String, count() as c:UInt64                                                                                                                                                        │
│     Sort: count():UInt64                                                                                                                                                                                                             │
│       AggregatorFinal: groupBy=[[OriginCityName, DestCityName]], aggr=[[count()]]                                                                                                                                                    │
│         AggregatorPartial: groupBy=[[OriginCityName, DestCityName]], aggr=[[count()]]                                                                                                                                                │
│           ReadDataSource: scan schema: [OriginCityName:String, DestCityName:String], statistics: [read_rows: 202687655, read_bytes: 8577734458, partitions_scanned: 203, partitions_total: 203], push_downs: [projections: [15, 24]] │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

```

## Q13 (2.745 sec., 73.83 million rows/sec., 1.56 GB/sec.)

```sql
SELECT
    OriginCityName,
    count(*) AS c
FROM ontime
GROUP BY OriginCityName
ORDER BY c DESC
LIMIT 10
```
```text
+-----------------------+----------+
| OriginCityName        | c        |
+-----------------------+----------+
| Chicago, IL           | 12592771 |
| Atlanta, GA           | 10944276 |
| Dallas/Fort Worth, TX |  9045390 |
| Houston, TX           |  6868696 |
| Los Angeles, CA       |  6726120 |
| New York, NY          |  6336818 |
| Denver, CO            |  6311909 |
| Phoenix, AZ           |  5678632 |
| Washington, DC        |  5022328 |
| San Francisco, CA     |  4696887 |
+-----------------------+----------+
10 rows in set (3.47 sec)
Read 202687655 rows, 4.29 GB in 2.745 sec., 73.83 million rows/sec., 1.56 GB/sec.
```

```text
┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Limit: 10                                                                                                                                                                                                   │
│   Projection: OriginCityName:String, count() as c:UInt64                                                                                                                                                    │
│     Sort: count():UInt64                                                                                                                                                                                    │
│       AggregatorFinal: groupBy=[[OriginCityName]], aggr=[[count()]]                                                                                                                                         │
│         AggregatorPartial: groupBy=[[OriginCityName]], aggr=[[count()]]                                                                                                                                     │
│           ReadDataSource: scan schema: [OriginCityName:String], statistics: [read_rows: 202687655, read_bytes: 4288897398, partitions_scanned: 203, partitions_total: 203], push_downs: [projections: [15]] │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Q14

```sql
SELECT
       count(*)
FROM ontime;
```

```text
+-----------+
| count()   |
+-----------+
| 202687655 |
+-----------+
1 row in set (0.44 sec)
```

```text
┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Projection: count():UInt64                                                                                                              │
│   Projection: 202687655 as count():UInt64                                                                                               │
│     Expression: 202687655:UInt64 (Exact Statistics)                                                                                     │
│       ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1] │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Block Statistics

```sql
CALL system$fuse_snapshot('default', 'ontime');
```

```text
+----------------------------------+------------------+---------------+-------------+-----------+--------------------+------------------+
| snapshot_id                      | prev_snapshot_id | segment_count | block_count | row_count | bytes_uncompressed | bytes_compressed |
+----------------------------------+------------------+---------------+-------------+-----------+--------------------+------------------+
| dd5685ae857f4cbb804d76b0513a0ff9 | NULL             |             1 |         203 | 202687655 |       146705866574 |       8891053905 |
+----------------------------------+------------------+---------------+-------------+-----------+--------------------+------------------+
```
