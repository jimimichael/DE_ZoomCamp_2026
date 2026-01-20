
import pandas as pd
from sqlalchemy import create_engine
import time

print("=== STARTING DATA LOAD ===")

# 1. Wait for database
time.sleep(5)

# 2. Connect to PostgreSQL
# Format: postgresql://username:password@host:port/database
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# 3. Test connection
print("Testing database connection...")
try:
    with engine.connect() as conn:
        print("Connected to database!")
except:
    print("Cannot connect to database")
    print("Make sure container is running: docker ps")
    exit(1)

# 4. Load taxi trips data
print("\nLoading taxi trips data...")
trips = pd.read_parquet('green_tripdata_2025-11.parquet')
trips.to_sql('green_trips', engine, if_exists='replace', index=False)
print(f"Loaded {len(trips)} trips")

# 5. Load taxi zones data
print("\nLoading taxi zones data...")
zones = pd.read_csv('taxi_zone_lookup.csv')
zones.to_sql('zones', engine, if_exists='replace', index=False)
print(f"Loaded {len(zones)} zones")

print("\n" + "="*40)
print("DATA LOAD COMPLETE!")
print("="*40)
print("\nYou can now run SQL queries.")
print("\nTo connect to database, run:")
print("  docker exec -it homework-postgres psql -U root -d ny_taxi")
