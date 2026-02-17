#!/usr/bin/env python3
"""
Load NYC taxi data into DuckDB from GCP Storage using httpfs.
Reads parquet files directly from Google Cloud Storage without downloading.
"""

import duckdb
import sys

# Database path
DB_PATH = "taxi_data.duckdb"
SCHEMA = "prod"

# GCP Storage URLs for taxi data
# Update BUCKET_NAME to your actual GCP bucket
BUCKET_NAME = "module_3_om"  # Change this to your bucket name

DATA_URLS = {
    "yellow_tripdata": f"gs://{BUCKET_NAME}/yellow_tripdata/*.parquet",
    "green_tripdata": f"gs://{BUCKET_NAME}/green_tripdata/*.parquet",
}

def connect_db():
    """Connect to DuckDB and enable httpfs extension"""
    conn = duckdb.connect(DB_PATH)
    try:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        print("✓ httpfs extension loaded")
    except Exception as e:
        print(f"⚠ httpfs may already be loaded: {e}")
    return conn

def create_schema(conn):
    """Create the prod schema if it doesn't exist"""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    print(f"✓ Schema '{SCHEMA}' ready")

def load_parquet_from_gcp(conn, table_name, gcp_url):
    """Load parquet file directly from GCP Storage into DuckDB"""
    try:
        print(f"⏳ Reading {gcp_url}...")
        
        # Read parquet directly from GCP and insert into DuckDB table
        # union_by_name=true handles schema differences across multiple parquet files
        conn.execute(f"""
            CREATE OR REPLACE TABLE {SCHEMA}.{table_name} AS 
            SELECT * FROM read_parquet('{gcp_url}', union_by_name=true)
        """)
        
        # Get row count
        result = conn.execute(f"SELECT COUNT(*) as row_count FROM {SCHEMA}.{table_name}").fetchall()
        row_count = result[0][0]
        
        print(f"✓ Loaded {row_count:,} rows into {SCHEMA}.{table_name}")
        return True
    except Exception as e:
        print(f"✗ Failed to load {table_name} from {gcp_url}")
        print(f"  Error: {e}")
        print(f"\n  💡 Troubleshooting:")
        print(f"     1. Verify the GCP bucket name: {BUCKET_NAME}")
        print(f"     2. Check that the parquet files exist in the bucket")
        print(f"     3. Ensure the files are publicly accessible or properly authenticated")
        return False

def main():
    """Main function"""
    print("=" * 70)
    print("📊 NYC Taxi Data Loader for DuckDB (reading from GCP Storage)")
    print("=" * 70)
    
    # Connect to DuckDB and load httpfs
    conn = connect_db()
    print(f"✓ Connected to {DB_PATH}")
    
    # Create schema
    create_schema(conn)
    
    # Load data from GCP
    print(f"\n📂 Loading data from GCP bucket: {BUCKET_NAME}")
    success = True
    for table_name, gcp_url in DATA_URLS.items():
        if not load_parquet_from_gcp(conn, table_name, gcp_url):
            success = False
    
    # Verify tables
    print(f"\n📋 Verifying tables in {SCHEMA} schema:")
    try:
        tables = conn.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{SCHEMA}'
            ORDER BY table_name
        """).fetchall()
        
        if tables:
            for table in tables:
                count = conn.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table[0]}").fetchone()[0]
                print(f"  • {table[0]}: {count:,} rows")
        else:
            print("  ⚠ No tables found in prod schema")
            success = False
    except Exception as e:
        print(f"✗ Error verifying tables: {e}")
        success = False
    
    # Close connection
    conn.close()
    
    print("\n" + "=" * 70)
    if success:
        print("✓ Data successfully loaded from GCP! You can now run 'dbt run'")
        print("\n  Next step:")
        print("  cd /workspaces/DE_ZoomCamp_2026/Module-4/dtc_dbt_project/taxi_rides_ny")
        print("  dbt run")
    else:
        print("⚠ Some errors occurred. Check the output above.")
        print("\n💡 To use local files instead, download from NYC TLC:")
        print("   https://d37ci6vzurychx.cloudfront.net/trip-data/")
    print("=" * 70)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
