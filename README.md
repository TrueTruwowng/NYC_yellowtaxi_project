# NYC Yellow Taxi Data Pipeline

An end-to-end data engineering project that processes **NYC Yellow Taxi trip data** using **Apache Spark on Databricks**. The pipeline follows the **Medallion Architecture** (Landing → Bronze → Silver → Gold) to incrementally cleanse, enrich, and aggregate taxi trip records for analytical use.

## Data Source

- **NYC Taxi & Limousine Commission (TLC)** trip record data, sourced from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) portal.
- **Yellow taxi trip records** for January–October 2025 (Parquet format).
- **Taxi Zone Lookup** reference table (CSV) mapping location IDs to borough and zone names.

## Architecture

The project is organized into four layers following the Medallion Architecture pattern, each implemented as Databricks notebooks written in **PySpark**:

```
Landing (00_landing)  →  Bronze (01_bronze)  →  Silver (02_silver)  →  Gold (03_gold)
```

All data is managed through **Unity Catalog** under the `nyctaxi` catalog and stored as **Delta tables**.

## Pipeline Layers

### 🟡 Landing (`00_landing/`)

Raw data ingestion from external sources into Databricks Volumes.

| Notebook | Description |
|---|---|
| `backfill-historical-yellow-trips` | Downloads monthly Yellow Taxi Parquet files (Jan–Oct 2025) from the TLC CloudFront CDN into `/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/` |
| `load-taxizone-lookup` | Downloads the taxi zone lookup CSV into `/Volumes/nyctaxi/00_landing/data_sources/lookup/` |

### 🟤 Bronze (`01_bronze/`)

Raw data loaded into Delta tables with minimal transformation.

| Notebook | Description |
|---|---|
| `bronze_processing` | Reads all landed Parquet files, adds a `processed_timestamp` column, and writes to `nyctaxi.01_bronze.yellow_trips_raw` |

### ⚪ Silver (`02_silver/`)

Cleansed, standardized, and enriched data ready for analysis.

| Notebook | Description |
|---|---|
| `lookup-table-processing` | Reads the taxi zone CSV, renames columns, casts data types, and adds SCD-style `effective_date` / `end_date` columns. Writes to `nyctaxi.02_silver.taxi_zone_lookup` |
| `yellow_trip_cleansed` | Filters trips to Jan–Oct 2025, decodes coded fields (`VendorID`, `RatecodeID`, `payment_type`) into human-readable values, computes `trip_duration` in minutes, and standardizes column names. Writes to `nyctaxi.02_silver.yellow_trips_cleansed` |
| `yellow_trip_enriched` | Joins cleansed trips with the taxi zone lookup table twice (pickup and dropoff) to add borough and zone names. Writes to `nyctaxi.02_silver.yellow_trips_enriched` |

### 🟡 Gold (`03_gold/`)

Business-level aggregations optimized for reporting and dashboards.

| Notebook | Description |
|---|---|
| `daily_trip_summary` | Aggregates enriched trip data by pickup date, computing `total_trips`, `avg_passengers_per_trip`, `avg_distance_per_trip`, `avg_fare_per_trip`, `max_fare`, `min_fare`, and `total_revenue`. Writes to `nyctaxi.03_gold.daily_trip_summary` |

## Tech Stack

| Component | Technology |
|---|---|
| Platform | Databricks |
| Processing Engine | Apache Spark (PySpark) |
| Storage Format | Delta Lake |
| Data Catalog | Unity Catalog |
| Language | Python |
| Source Format | Parquet, CSV |

## Data Flow Diagram

```
TLC CloudFront CDN
        │
        ▼
┌───────────────────┐
│  00_landing        │  Raw Parquet & CSV files → Databricks Volumes
└────────┬──────────┘
         ▼
┌───────────────────┐
│  01_bronze         │  yellow_trips_raw (+ processed_timestamp)
└────────┬──────────┘
         ▼
┌───────────────────┐
│  02_silver         │  taxi_zone_lookup
│                    │  yellow_trips_cleansed (decoded, filtered, standardized)
│                    │  yellow_trips_enriched (joined with zone lookup)
└────────┬──────────┘
         ▼
┌───────────────────┐
│  03_gold           │  daily_trip_summary (aggregated metrics by date)
└───────────────────┘
```

## How to Run

1. **Set up a Databricks workspace** with Unity Catalog enabled.
2. **Create the catalog and schemas**:
   ```sql
   CREATE CATALOG IF NOT EXISTS nyctaxi;
   CREATE SCHEMA IF NOT EXISTS nyctaxi.00_landing;
   CREATE SCHEMA IF NOT EXISTS nyctaxi.01_bronze;
   CREATE SCHEMA IF NOT EXISTS nyctaxi.02_silver;
   CREATE SCHEMA IF NOT EXISTS nyctaxi.03_gold;
   ```
3. **Create a Volume** for landing data:
   ```sql
   CREATE VOLUME IF NOT EXISTS nyctaxi.00_landing.data_sources;
   ```
4. **Run the notebooks in order**:
   1. `00_landing/backfill-historical-yellow-trips`
   2. `00_landing/load-taxizone-lookup`
   3. `01_bronze/bronze_processing`
   4. `02_silver/lookup-table-processing`
   5. `02_silver/yellow_trip_cleansed`
   6. `02_silver/yellow_trip_enriched`
   7. `03_gold/daily_trip_summary`

## Output Tables

| Table | Schema | Description |
|---|---|---|
| `nyctaxi.01_bronze.yellow_trips_raw` | Bronze | Raw trip data with ingestion timestamp |
| `nyctaxi.02_silver.taxi_zone_lookup` | Silver | Cleaned taxi zone reference data |
| `nyctaxi.02_silver.yellow_trips_cleansed` | Silver | Filtered and decoded trip records |
| `nyctaxi.02_silver.yellow_trips_enriched` | Silver | Trips enriched with pickup/dropoff zone names |
| `nyctaxi.03_gold.daily_trip_summary` | Gold | Daily aggregated trip metrics |
