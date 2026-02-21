#!/usr/bin/env python3
"""
Load FHV (For-Hire Vehicle) trip data for 2019 into DuckDB.
Downloads data from NYC TLC GitHub releases for all 12 months.
"""

import duckdb
import sys

# Database path
DB_PATH = "taxi_data.duckdb"
SCHEMA = "prod"

# FHV data URLs for 2019 (all 12 months)
MONTHS = [f"{i:02d}" for i in range(1, 13)]
FHV_BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-"

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

def load_fhv_data(conn):
    """Load FHV data for all months of 2019"""
    try:
        print("⏳ Loading FHV data for 2019 (all 12 months)...")
        
        # Build CSV URLs for all months
        fhv_urls = [f"{FHV_BASE_URL}{month}.csv.gz" for month in MONTHS]
        url_pattern = "{" + ",".join(f"'{url}'" for url in fhv_urls) + "}"
        
        # Create raw FHV table from all CSV files
        conn.execute(f"""
            CREATE OR REPLACE TABLE {SCHEMA}.raw_fhv_tripdata AS 
            SELECT * FROM read_csv_auto([{', '.join(f"'{url}'" for url in fhv_urls)}])
        """)
        
        # Get row count
        result = conn.execute(f"SELECT COUNT(*) as row_count FROM {SCHEMA}.raw_fhv_tripdata").fetchall()
        row_count = result[0][0]
        
        print(f"✓ Loaded {row_count:,} FHV trip records into {SCHEMA}.raw_fhv_tripdata")
        return True
    except Exception as e:
        print(f"✗ Failed to load FHV data")
        print(f"  Error: {e}")
        print(f"\n  💡 Troubleshooting:")
        print(f"     1. Ensure httpfs extension is properly loaded")
        print(f"     2. Check internet connection to GitHub")
        print(f"     3. Verify the CSV files are still available at the URLs")
        return False

def verify_schema(conn):
    """Verify FHV data was loaded correctly"""
    print(f"\n📋 FHV Data Summary:")
    try:
        # Get row count
        count = conn.execute(f"SELECT COUNT(*) FROM {SCHEMA}.raw_fhv_tripdata").fetchone()[0]
        print(f"  • Total records: {count:,}")
        
        # Get column info
        cols = conn.execute(f"SELECT * FROM {SCHEMA}.raw_fhv_tripdata LIMIT 0").description
        print(f"  • Columns ({len(cols)}):")
        for col in cols[:5]:
            print(f"    - {col[0]} ({col[1]})")
        if len(cols) > 5:
            print(f"    - ... and {len(cols) - 5} more")
        
        # Check for null dispatching_base_num
        null_count = conn.execute(
            f"SELECT COUNT(*) FROM {SCHEMA}.raw_fhv_tripdata WHERE dispatching_base_num IS NULL"
        ).fetchone()[0]
        print(f"  • Records with NULL dispatching_base_num: {null_count:,} (will be filtered in staging)")
        
    except Exception as e:
        print(f"✗ Error verifying data: {e}")
        return False
    
    return True

def main():
    """Main function"""
    print("=" * 70)
    print("🚗 FHV Trip Data Loader for DuckDB (2019)")
    print("=" * 70)
    
    # Connect to DuckDB
    conn = connect_db()
    print(f"✓ Connected to {DB_PATH}")
    
    # Create schema
    create_schema(conn)
    
    # Load FHV data
    print(f"\n📂 Loading FHV data from GitHub...")
    success = load_fhv_data(conn)
    
    # Verify
    if success:
        verify_schema(conn)
    
    # Close connection
    conn.close()
    
    print("\n" + "=" * 70)
    if success:
        print("✓ FHV data successfully loaded!")
        print("\n  Next step: Create dbt staging model")
        print("  cd /workspaces/DE_ZoomCamp_2026/Module-4/dtc_dbt_project/taxi_rides_ny")
        print("  dbt run")
    else:
        print("⚠ Some errors occurred. Check the output above.")
    print("=" * 70)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
