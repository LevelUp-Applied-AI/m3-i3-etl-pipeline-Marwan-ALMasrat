[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

This pipeline extracts data from a PostgreSQL database for the fictional
Amman Digital Market e-commerce platform, transforms it into a
customer-level analytics summary, validates data quality, and loads the
result into a new database table and CSV file.

The pipeline performs the following steps:
- **Extract**: Reads 4 tables (customers, products, orders, order_items)
- **Transform**: Joins tables, computes revenue, filters bad data, aggregates per customer
- **Validate**: Runs quality checks and raises an error if any check fails
- **Load**: Writes results to PostgreSQL table `customer_analytics` and `output/customer_analytics.csv`

## Setup

1. Start PostgreSQL container:
```bash
   docker run -d --name postgres-m3-int -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=amman_market -p 5433:5432 -v pgdata_m3_int:/var/lib/postgresql/data postgres:15-alpine
```
2. Load schema and data:
```bash
   Get-Content schema.sql | docker exec -i postgres-m3-int psql -U postgres -d amman_market
   Get-Content seed_data.sql | docker exec -i postgres-m3-int psql -U postgres -d amman_market
```
3. Install dependencies:
```bash
   pip install -r requirements.txt
```

## How to Run
```bash
python etl_pipeline.py
```

## Output

`output/customer_analytics.csv` contains one row per customer with these columns:

| Column | Description |
|--------|-------------|
| customer_id | Unique customer identifier |
| customer_name | Full name of the customer |
| city | Customer's city |
| total_orders | Number of distinct orders placed |
| total_revenue | Sum of all line totals (quantity × unit_price) |
| avg_order_value | total_revenue / total_orders |
| top_category | Product category with highest revenue for this customer |

## Quality Checks

| Check | Why |
|-------|-----|
| No nulls in `customer_id` | Every row must belong to a known customer |
| No nulls in `customer_name` | Name is required for the analytics report |
| `total_revenue > 0` | Customers with zero revenue should not appear |
| No duplicate `customer_id` | One row per customer — groupBy must be correct |
| `total_orders > 0` | Every customer must have at least one valid order |

If any check fails, the pipeline raises a `ValueError` and stops.

---

## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice,
and reference code you wrote here in your professional portfolio.
Redistribution outside this course is not permitted.