"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: TODO_col1
    type: TODO_type
    description: TODO

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    import os
    import json
    from datetime import datetime, timedelta

    import pandas as pd

    # Read window variables provided by Bruin runtime
    start_dt = os.environ.get("BRUIN_START_DATETIME") or os.environ.get("BRUIN_START_DATE")
    end_dt = os.environ.get("BRUIN_END_DATETIME") or os.environ.get("BRUIN_END_DATE")
    if not start_dt or not end_dt:
      # Nothing to do, return empty list
      return []

    # Normalize to datetimes
    try:
      if len(start_dt) == 10:
        start = datetime.fromisoformat(start_dt + "T00:00:00")
      else:
        start = datetime.fromisoformat(start_dt)
      if len(end_dt) == 10:
        end = datetime.fromisoformat(end_dt + "T00:00:00")
      else:
        end = datetime.fromisoformat(end_dt)
    except Exception:
      return []

    # Read pipeline variables from BRUIN_VARS (JSON)
    taxi_types = ["yellow", "green"]
    vars_json = os.environ.get("BRUIN_VARS")
    if vars_json:
      try:
        parsed = json.loads(vars_json)
        taxi_types = parsed.get("taxi_types", taxi_types)
      except Exception:
        pass

    # Build a small synthetic dataset: one row per day per taxi_type
    rows = []
    cur = start
    while cur < end:
      for t in taxi_types:
        rows.append(
          {
            "pickup_datetime": cur,
            "taxi_type": t,
            "passenger_count": 1,
            "trip_distance": 1.0,
            "payment_type_id": 1,
          }
        )
      cur = cur + timedelta(days=1)

    df = pd.DataFrame(rows)
    return df


