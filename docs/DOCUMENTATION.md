## ENVIRONMENT VARIABLES

Copy `.env.example` (repository root) to `.env` and replace the placeholder values.
`docker-compose.yml` reads these variables:

```yaml
# ---------- POSTGRES ----------
POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

# ---------- AIRFLOW ADMIN ----------
AIRFLOW_ADMIN_USER, AIRFLOW_ADMIN_FIRSTNAME, AIRFLOW_ADMIN_LASTNAME,
AIRFLOW_ADMIN_EMAIL, AIRFLOW_ADMIN_PASSWORD
```

## DATA SOURCE 

### API
```yaml
https://opensky-network.org/api/states/all
```

### API DOCUMENTATION
```yaml
https://openskynetwork.github.io/opensky-api/rest.html
```


## AIRFLOW PIPELINE DAG STRUCTURE FOR BRONZE 

```python
import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_layer import run_bronze_ingestion

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay" : timedelta(minutes=5),
}

with DAG(
    dag_id="flights_ops_medallion_pipe",
    default_args=default_args,
    start_date=datetime(2025, 12, 10),
    schedule_interval="*/30 * * * *",
    catchup=False,
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingestion,
    )

```

## SQL SCRIPT FOR DATABASE, SCHEMA AND TABLE CREATION

### DATABASE
```sql
CREATE DATABASE IF NOT EXISTS FLIGHTS;
```

### SCHEMA
```sql
CREATE SCHEMA IF NOT EXISTS FLIGHTS_SCHEMA;
```

### TABLE
```sql
USE SCHEMA FLIGHTS_SCHEMA;

CREATE TABLE FLIGHT_TABLE (
    window_start TIMESTAMP,
    origin_country TEXT,
    total_flights INT,
    avg_velocity FLOAT,
    on_ground INT,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, origin_country)
);

```


## SNOWFLAKE DASHBOARD SCRIPTS

### 1. Top 5 Countries by Total Flights
```sql
SELECT
    origin_country,
    SUM(total_flights) AS total_flights
FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE
GROUP BY origin_country
ORDER BY total_flights DESC
LIMIT 5;
```

### 2. Countries with Fastest Flights
```sql
SELECT
    origin_country,
    ROUND(AVG(avg_velocity),2) AS avg_speed
FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE
GROUP BY origin_country
ORDER BY avg_speed DESC
LIMIT 5;
```

### 3. Total Flights 

```sql
SELECT SUM(total_flights) AS total_flights
FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE;
```

### 4. ACTIVE Countries
```sql
SELECT COUNT(DISTINCT origin_country) AS countries
FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE;
```

### 5. Flight Activity Trend Over Time (Line Chart)
```sql
SELECT
    DATE(window_start) AS flight_date,
    SUM(total_flights) AS total_flights
FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE
GROUP BY DATE(window_start)
ORDER BY flight_date;
```