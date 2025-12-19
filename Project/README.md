# NYC Taxi Demand & Weather Dashboard

## Overview
This project implements a production-style Databricks Lakehouse pipeline to analyze NYC taxi demand and revenue patterns with daily weather data. The pipeline ingests public datasets, applies data quality validation, and produces KPI marts consumed by dashboards.

## Workflow
The pipeline follows a Bronze → Silver → Gold design pattern:

- Bronze: Raw ingestion of NYC TLC Yellow Taxi trip data and NOAA daily weather data, partitioned by year and month
- Silver: Cleaned and standardized tables with data quality validation
- Gold: Aggregated KPI marts for analytics and dashboard 

- Invalid or out-of-scope records are quarantined, enabling transparency and data quality monitoring.


![image](./images/work-flow.png)


![ETL Pipeline](./images/pipeline.png)
*The diagram above illustrates the ETL pipeline orchestrated by a Databricks job, which automates data ingestion, transformation, and loading throughout the Lakehouse workflow.*

## Data Quality
The Silver layer enforces multiple validation rules, including:

- Valid pickup and dropoff timestamps
- Reasonable trip durations and distances
- Non-negative monetary values
- Event-time consistency with source partitions

Records failing these checks are written to a quarantine table


## Analytics
The Gold layer provides the following analytics-ready tables:

### City-level daily KPIs

- Total trips
- Revenue
- Average fare, tip, distance, and duration
  
### Zone-level daily KPIs
- Demand and revenue by pickup zone and borough
- Weather-enriched daily metrics

### Data quality metrics
- Percentage of records excluded
- Breakdown by exclusion reason
  
## Dashboard
![image](./images/image.png)
The dashboard highlights demand trends, revenue performance, weather impacts, and data quality health to support analytical and operational insights.