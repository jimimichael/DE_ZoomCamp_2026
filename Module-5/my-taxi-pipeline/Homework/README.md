# Question 1
.bruin.yml and pipeline.yml (assets can be anywhere)

# Question 2
replace - truncate and rebuild entirely

# Question 3 - Override taxi_types
bruin run ./pipeline/pipeline.yml --var 'taxi_types=["yellow"]'

# Question 4 - Run with dependencies
bruin run ./pipeline/assets/ingestion/trips.py --downstream

# Question 6 - View lineage
bruin lineage ./pipeline/assets/ingestion/trips.py

# Question 7 - First-time run with full refresh
bruin run ./pipeline/pipeline.yml --full-refresh



