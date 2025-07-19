# Urban Mobility & Socioeconomic Analysis: NYC Taxi vs. Uber


Analyze how income, property value, and population density impact the choice between Yellow Taxi and Uber rides across Manhattan.  Built with **Apache Spark**, **Snowflake**, and **Power BI**.

---

## Stages

| Stage         | Highlights                                                                                                                                          |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Ingest**    | • Downloads monthly TLC Parquet files • Handles Yellow Taxi (`yellow_tripdata_YYYY‑MM.parquet`) & High‑Volume FHV (`fhvhv_tripdata_YYYY‑MM.parquet`)|
| **Clean**     | • Drops bad rows, normalizes schemas, decodes `payment_type` • Geo‑join with TLC Taxi Zones shapefile                                               |
| **Transform** | • Classifies every trip into **Zones and Regions**                                                                                                  |
| **Aggregate** | • Generates BI‑ready tables                                                                                                                         |
| **Load**      | • Writes summaries to Snowflake (`NYC_MOBILITY` DB) via Spark connector                                                                             |
| **Visualize** | • Power BI dashboards                                                                                                                               |

---

## Repository Layout

```
├── data/
│   ├── raw/                # Monthly parquet from TLC & FHV
│   ├── cleaned/            # yearly Parquet TLC & FHV
│   ├── external/           # Taxi zones shp, Census CSVs
│   ├── outputs/            # BI ready local exports
├── scripts/
│   ├── census/             # Manual census data generation  
│   ├── exploration/        # Exploration of created scripts
│   ├── taxi/               # Taxi data processing
│   ├── transform/          # Join taxi with Uber data and filter Manhattan zones and regions
│   ├── uber/               # Uber data processing
│   └── load_to_snowflake.py                     
├── .env                    # Snowflake creds template
├── visuals/                # Power BI dashboards files
├── requirements.txt
└── README.md 
```

---

## Local Spark Setup

### Python

- Version: **3.9.13**
- Path: `C:/python39`

### Java

- Version: `openjdk version "1.8.0_452"`
- Path: `C:\java\jdk-8`
- Download: [https://adoptium.net/en-GB/temurin/releases/?os=any&arch=any&version=8](https://adoptium.net/en-GB/temurin/releases/?os=any\&arch=any\&version=8)

### Spark

- Version: **3.5.5**
- Path: `C:\spark`
- Download: [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)

#### Snowflake Connector Jars

Place the following in `C:\spark\jars`:

- `jackson-databind-2.19.0.jar`
- `snowflake-jdbc-3.16.1.jar`
- `spark-snowflake_2.12-3.0.0.jar`

JAR Links:

- [https://mvnrepository.com/artifact/net.snowflake/spark-snowflake\_2.12/3.0.0](https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.12/3.0.0)
- [https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.16.1/](https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.16.1/)

### Hadoop

- Path: `C:\hadoop\bin`
- Required files:
  - `hadoop.dll`
  - `winutil.exe`

Download: [https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin)

---

## Quick Start

```bash
# 1. Create & activate venv
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt

# 2. Set your Snowflake credentials
cp .env .env  # then fill values
```

---
