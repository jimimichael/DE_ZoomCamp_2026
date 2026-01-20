
# Module 1 Homework Solutions

# Module 1 Homework - Data Engineering Zoomcamp 2026

## Question 1: Understanding Docker Images

**Command Used**:
```bash
docker run --rm python:3.13 pip --version

**Answer**: 25.3 

## Question 2: Docker Networking
From the docker-compose file, pgadmin connects to:
Hostname: db (service name)
Port: 5432 (container port)

**Answer**:db:5432

Explanation:
In Docker Compose, containers use service names as hostnames within the network.
The PostgreSQL service is named db, and it listens on port 5432 inside the container.
The host port 5433 is for external access only.


## Question 3. Short trips count
SQL Query:
-- Count trips with distance <= 1 mile in November 2025
SELECT COUNT(*) 
FROM green_trips 
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;

**Answer**: 8007


## Question 4: Longest trip each day
SQL Query:
-- Find day with longest trip (under 100 miles)
SELECT DATE(lpep_pickup_datetime) as day,
       MAX(trip_distance) as max_distance
FROM green_trips
WHERE trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;

**Answer**:     
day : 2025-11-14 
max_distance: 88.03



## Question 5: Biggest pickup zone (Nov 18)
SQL Query:
-- Find zone with highest total amount on Nov 18
SELECT 
    z."Zone" AS pickup_zone,
    SUM(g.total_amount) AS total_amount_sum
FROM green_trips g
JOIN zones z 
  ON g."PULocationID" = z."LocationID"
WHERE g.lpep_pickup_datetime >= '2025-11-18'
  AND g.lpep_pickup_datetime <  '2025-11-19'
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1;

**Answer**: 
pickup_zone: East Harlem North 
total_amount_sum: $9281.92
 | 


## Question 6: Largest tip from East Harlem North

SQL Query:
-- Find dropoff zone with largest tip from East Harlem North
SELECT 
    dz."Zone" AS dropoff_zone,
    MAX(g.tip_amount) AS largest_tip
FROM green_trips g
JOIN zones pz 
  ON g."PULocationID" = pz."LocationID"   -- Pickup zone
JOIN zones dz 
  ON g."DOLocationID" = dz."LocationID"   -- Dropoff zone
WHERE pz."Zone" = 'East Harlem North'
GROUP BY dz."Zone"
ORDER BY largest_tip DESC
LIMIT 1;

**Answer**: 
dropoff_zone: Yorkville West
largest_tip: 81.89



## Question 7: Terraform Workflow

**Answer**: terraform init, terraform apply -auto-approve, terraform destroy


Files Included:
README.md - This file with answers

green_tripdata_2025-11.parquet - Green taxi data for November 2025

taxi_zone_lookup.csv - Taxi zone lookup data

load_file.py - Python script to load data into PostgreSQL

How to Run:
Start PostgreSQL: docker run -d --name homework-postgres -e POSTGRES_USER=root -e POSTGRES_PASSWORD=root -e POSTGRES_DB=ny_taxi -p 5432:5432 postgres:18

Load data: python load_file.py

Connect to database: docker exec -it homework-postgres psql -U root -d ny_taxi

