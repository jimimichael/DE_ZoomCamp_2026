# Module 6 Homework (06_homework.ipynb)

This README reproduces every step (code + output) from `06_homework.ipynb` in this folder.

---
```python
#import module & create spark session

from pyspark.sql import SparkSession
import pyspark.sql.types as types
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('module6_homework') \
    .getOrCreate()

print(f"Spark version: {spark.version}")
```
```text
Spark version: 4.1.1
```

```python
#download homework data

!wget -O yellow_tripdata_2025-11.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
!wget -O taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

```python
# Check files downloaded
!ls -lh *.parquet *.csv
```
```text
-rw-r--r-- 1 olujimim olujimim 295M Jun 30  2022 fhvhv_tripdata_2021-01.parquet
-rw-r--r-- 1 olujimim olujimim 262K Mar  9 08:44 head.csv
-rw-r--r-- 1 olujimim olujimim  13K Feb 22  2024 taxi_zone_lookup.csv
-rw-r--r-- 1 olujimim olujimim  68M Dec 19 15:51 yellow_tripdata_2025-11.parquet
```

```python
#read the Parquet file Without a schema first to see what is inside
df_raw = spark.read.parquet('yellow_tripdata_2025-11.parquet')

#view structure: volumn names and types
df_raw.printSchema()

#show first 5 rows of actual data
df_raw.show(5)

#count total rows
print(f"\n TOTAL ROWS: {df_raw.count():,}")
```
```text
root
 |-- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)
 |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)
 |-- passenger_count: long (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: long (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: long (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- Airport_fee: double (nullable = true)
 |-- cbd_congestion_fee: double (nullable = true)

+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|cbd_congestion_fee|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|       7| 2025-11-01 00:13:25|  2025-11-01 00:13:25|              1|         1.68|         1|                 N|          43|         186|           1|       14.9|  0.0|    0.5|       1.5|         0.0|                  1.0|       22.15|                 2.5|        0.0|              0.75|
|       2| 2025-11-01 00:49:07|  2025-11-01 01:01:22|              1|         2.28|         1|                 N|         142|         237|           1|       14.2|  1.0|    0.5|      4.99|         0.0|                  1.0|       24.94|                 2.5|        0.0|              0.75|
|       1| 2025-11-01 00:07:19|  2025-11-01 00:20:41|              0|          2.7|         1|                 N|         163|         238|           1|       15.6| 4.25|    0.5|      4.27|         0.0|                  1.0|       25.62|                 2.5|        0.0|              0.75|
|       2| 2025-11-01 00:00:00|  2025-11-01 01:01:03|              3|        12.87|         1|                 N|         138|         261|           1|       66.7|  6.0|    0.5|       0.0|        6.94|                  1.0|       86.14|                 2.5|       1.75|              0.75|
|       1| 2025-11-01 00:18:50|  2025-11-01 00:49:32|              0|          8.4|         1|                 N|         138|          37|           2|       39.4| 7.75|    0.5|       0.0|         0.0|                  1.0|       48.65|                 0.0|       1.75|               0.0|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
only showing top 5 rows

 TOTAL ROWS: 4,181,444
```

```python
#Question 1
```

```python
print(spark.version)
```
```text
4.1.1
```

```python
#Question 2
#Average size of the Parquet in MB
```

```python
#Repartition to 4
df_repartitioned = df_raw.repartition(4)

#save to a new folder
df_repartitioned.write.parquet('yellow_tripdata_2025-11_4partitions', mode='overwrite')
```

```python
#check file sizes

import os
import glob

#find all parquet files in folder
parquet_files = glob.glob('yellow_tripdata_2025-11_4partitions/part-*.parquet')

#calculate average size
total_size = 0
for file in parquet_files:
    size_mb = os.path.getsize(file) / (1024 * 1024)  #converts bytes to MB
    print(f"{os.path.basename(file)}: {size_mb: .2f} MB")
    total_size += size_mb

#get avg_size
avg_size = total_size / len(parquet_files)
print(f"\n Average File Size: {avg_size: .1f} MB")
```
```text
part-00003-4b79179d-dcfc-46a1-ab33-72406ee6d225-c000.snappy.parquet:  24.42 MB
part-00000-4b79179d-dcfc-46a1-ab33-72406ee6d225-c000.snappy.parquet:  24.41 MB
part-00002-4b79179d-dcfc-46a1-ab33-72406ee6d225-c000.snappy.parquet:  24.42 MB
part-00001-4b79179d-dcfc-46a1-ab33-72406ee6d225-c000.snappy.parquet:  24.41 MB

 Average File Size:  24.4 MB
```

```python
#Question 3: Count records
#How many trips were there on thd 15th November?
```

```python
#using date filter
from pyspark.sql import functions as F

#create a date column for easier filtering
df_with_date = df_raw.withColumn(
    'pickup_date',
    F.to_date('tpep_pickup_datetime') #convert timestamp to date
)

#count trips where pickup date is Nov 15, 2025
Nov15_trip_count = df_with_date.filter(F.col('pickup_date') == '2025-11-15').count()

#print count
print(f"Trips on Nov. 15, 2025: {Nov15_trip_count: ,}")
```
```text
Trips on Nov. 15, 2025:  162,604
```

```python
#Question 4: Longest trip
```

```python
#calculate trip duration in hours
df_duration = df_raw.withColumn(
    'trip_duration_hours',
    (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 3600
)

#find max duration
max_duration = df_duration.agg(F.max('trip_duration_hours')).collect()[0][0]

print(f"Longest trip: {max_duration: .1f} hours")
```
```text
Longest trip:  90.6 hours
```

```python
#Question 6. Least frequent pickup location zone
```

```python
#read zone lookup data
df_zones = spark.read.option('header', 'true').csv('taxi_zone_lookup.csv')

#view data
df_zones.show(5)
```
```text
+----------+-------------+--------------------+------------+
|LocationID|      Borough|                Zone|service_zone|
+----------+-------------+--------------------+------------+
|         1|          EWR|      Newark Airport|         EWR|
|         2|       Queens|         Jamaica Bay|   Boro Zone|
|         3|        Bronx|Allerton/Pelham G...|   Boro Zone|
|         4|    Manhattan|       Alphabet City| Yellow Zone|
|         5|Staten Island|       Arden Heights|   Boro Zone|
+----------+-------------+--------------------+------------+
only showing top 5 rows
```

```python
#count trips per pickkup location ID
pickup_counts = df_raw.groupBy('PULocationID').count()
#Trip count per location ID (first 5)

pickup_counts.show(5)
```
```text
+------------+-----+
|PULocationID|count|
+------------+-----+
|         148|51711|
|         243| 4901|
|          31|   89|
|         137|46493|
|          85| 1711|
+------------+-----+
only showing top 5 rows
```

```python
#Join with zones
df_with_zone_names = pickup_counts.join(
    df_zones,
    pickup_counts.PULocationID == df_zones.LocationID,
    'inner'
).select('Zone', 'count')

#find least frequent zones (samllest count)
least_frequent = df_with_zone_names.orderBy('count').first()

print(f"Least frequent pickup zone: {least_frequent['Zone']}")

print(f"\n (only {least_frequent['count']} trips")


#Check all options to see which is correct
options = [
    "Governor's Island/Ellis Island/Liberty Island",
    "Arden Heights",
    "Rikers Island",
    "Jamaica Bay"
]


print("\n Checking options: ")
for zones in options:
    count = df_with_zone_names.filter(F.col('Zone') == zones).select('count').collect()

    if count:
        print(f"{zones}: {count[0]['count']} trips")
    else:
        print(f"{zones}: 0 trips (not found)")



#Double-check which one has the smallest number
print("\n All Zones Sorted By Count (smallest first):")
df_with_zone_names.orderBy('count').show(15, truncate=False)
```
```text
Least frequent pickup zone: Governor's Island/Ellis Island/Liberty Island

 (only 1 trips

 Checking options: 
Governor's Island/Ellis Island/Liberty Island: 1 trips
Arden Heights: 1 trips
Rikers Island: 4 trips
Jamaica Bay: 5 trips

 All Zones Sorted By Count (smallest first):
+---------------------------------------------+-----+
|Zone                                         |count|
+---------------------------------------------+-----+
|Governor's Island/Ellis Island/Liberty Island|1    |
|Arden Heights                                |1    |
|Eltingville/Annadale/Prince's Bay            |1    |
|Port Richmond                                |3    |
|Rossville/Woodrow                            |4    |
|Rikers Island                                |4    |
|Green-Wood Cemetery                          |4    |
|Great Kills                                  |4    |
|Jamaica Bay                                  |5    |
|Westerleigh                                  |12   |
|Oakwood                                      |14   |
|Crotona Park                                 |14   |
|New Dorp/Midland Beach                       |14   |
|West Brighton                                |14   |
|Willets Point                                |15   |
+---------------------------------------------+-----+
only showing top 15 rows
```

```python

```
