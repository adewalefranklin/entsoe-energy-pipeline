Architecture Overview

S3 (JSON files)
   ↓
Snowflake RAW (ENTSOE_RAW)
   ↓
dbt Staging
   ↓
Dimensions & Fact
   ↓
Analytics Mart (BI-ready view)


Lineage (dbt docs):

Source → Staging → Dimensions / Fact → Mart


📂 Project Structure

models/
├── staging/
│   └── stg_entsoe_day_ahead_prices.sql
├── dimensions/
│   ├── dim_bidding_zone.sql
│   └── dim_date.sql
├── facts/
│   └── fct_day_ahead_price.sql
└── marts/
    └── mart_day_ahead_prices.sql


🧩 Models Explained

🔹 Source

ENTSOE_RAW

Raw JSON data loaded from S3

Preserved as VARIANT for flexibility and traceability

🔹 Staging

stg_entsoe_day_ahead_prices

Parses JSON

Extracts:

bidding zone

delivery date

hourly position (1–24)

price (EUR/MWh)

Maintains source lineage (filename, load_time)

🔹 Dimensions
dim_bidding_zone

Maps bidding zone codes (e.g. DE-LU, SE4) to human-readable names

Improves usability for non-technical stakeholders

Enables geographic analysis in BI tools

dim_date

Standard date dimension

Includes:

year, quarter, month

day name

weekend flag

🔹 Fact
fct_day_ahead_price

Grain: one record per (zone, delivery_date, hour position)

Incremental model

Derives:

delivery_datetime

period_of_day (Early Hours, Morning, Midday, Evening, Night)

Preserves all source records (including duplicates) to reflect real market behavior

⚠️ Note: ENTSO-E may publish multiple prices per zone/hour due to corrections or re-publishing.
These are intentionally preserved in the fact table.

🔹 Mart (BI-ready)
mart_day_ahead_prices

View optimized for analytics and reporting

Joins fact + dimensions

Includes:

readable zone names

date attributes

period of day

measures (price EUR/MWh)

This is the recommended entry point for BI tools.


🧪 Data Quality & Testing


Implemented dbt tests at the mart level:

not_null tests for key business columns

Uniqueness is intentionally not enforced due to known source behavior

In a production setup, versioning or “latest price” logic could be added.


📘 Documentation


dbt documentation is generated using:

dbt docs generate

![alt text](dbt-linear-graph.png)


🛠️ Tech Stack


Snowflake – Cloud data warehouse

dbt Core – Data transformations, testing, and documentation

AWS S3 – Raw data storage (JSON files)

Python – API ingestion and data extraction (ENTSO-E day-ahead prices)

SQL – Analytical data modeling

Git / GitHub – Version control and project sharing

Python was used to retrieve ENTSO-E day-ahead price data via API and persist the raw responses to S3 prior to ingestion into Snowflake.


🚀 Future Improvements

Price versioning / late-arriving corrections

Latest-price snapshot logic

Airflow orchestration

Power BI dashboard built on the mart