# Module 6 Homework (06_homework.ipynb)

This README reproduces every step (code + comments) from `06_homework.ipynb` in this folder.

It covers:
- Creating a Spark session
- Downloading the November 2025 yellow taxi trip data (Parquet)
- Exploring the dataset (schema, sample rows, row count)
- Repartitioning the Parquet files and measuring file sizes
- Answering homework questions about trip counts and least frequent pickup zones

---

## 1) Setup Spark Session

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

## 2) Download homework data

```python
#download homework data

!wget -O yellow_tripdata_2025-11.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
!wget -O taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

## 3) Verify downloads

```python
# Check files downloaded
!ls -lh *.parquet *.csv
```

## 4) Read the Parquet file and explore

```python
#read the Parquet file Without a schema first to see what is inside
df_raw = spark.read.parquet('yellow_tripdata_2025-11.parquet')

#view structure: volume names and types
df_raw.printSchema()

#show first 5 rows of actual data
df_raw.show(5)

#count total rows
print(f"\n TOTAL ROWS: {df_raw.count():,}")
```

## 5) Question 1

```python
#Question 1
```

```python
print(spark.version)
```

## 6) Question 2

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
    size_mb = os.path.getsize(file) / (1024 * 1024)  # converts bytes to MB
    print(f"{os.path.basename(file)}: {size_mb: .2f} MB")
    total_size += size_mb

#get avg_size
avg_size = total_size / len(parquet_files)
print(f"\n Average File Size: {avg_size: .1f} MB")
```

## 7) Question 3: Count records

```python
#Question 3: Count records
#How many trips were there on the 15th November?
```

```python
#using date filter
from pyspark.sql import functions as F

#create a date column for easier filtering
df_with_date = df_raw.withColumn(
    'pickup_date',
    F.to_date('tpep_pickup_datetime')  # convert timestamp to date
)

#count trips where pickup date is Nov 15, 2025
Nov15_trip_count = df_with_date.filter(F.col('pickup_date') == '2025-11-15').count()

#print count
print(f"Trips on Nov. 15, 2025: {Nov15_trip_count: ,}")
```

## 8) Question 4: Longest trip

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

## 9) Question 6: Least frequent pickup location zone

```python
#Question 6. Least frequent pickup location zone 
```

```python
#read zone lookup data
df_zones = spark.read.option('header', 'true').csv('taxi_zone_lookup.csv')

#view data
df_zones.show(5)
```

```python
#count trips per pickup location ID
pickup_counts = df_raw.groupBy('PULocationID').count()
#Trip count per location ID (first 5)

pickup_counts.show(5)
```

```python
#Join with zones
df_with_zone_names = pickup_counts.join(
    df_zones,
    pickup_counts.PULocationID == df_zones.LocationID,
    'inner'
).select('Zone', 'count')

#find least frequent zones (smallest count)
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
