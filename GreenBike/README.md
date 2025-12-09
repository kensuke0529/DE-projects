# GreenBike Real-Time Data Pipeline (AWS)

A production-style data engineering project that ingests, processes, and analyzes real-time bike availability data from Salt Lake City’s GreenBike system.

## Overview
This pipeline collects GreenBike availability data every 10 minutes, stores in S3 data lake, transforms them by AWS Glue, and pass Parquet tables through Athena for Tableau (any BI tools).

## Architecture 
follows the standard Bronze → Silver → Gold model:
-  Bronze (Raw): JSON API snapshots
-  Silver (Curated): Cleaned + joined Parquet tables
-  Gold (Analytics): Athena SQL + dashboards

![Data Engineering Pipeline Architecture](/GreenBike//images/DE%20project.png)

## Features
1. Data Ingestion - AWS Lambda
2. Data Lake
3. ETL - AWS Glue
4. Schema Check - AWS Crawler
5. Query - AWS Athena

## Cost
| Service               | What It Does                              | Monthly Cost     |
| --------------------- | ----------------------------------------- | ---------------- |
| **AWS Glue ETL**      | 6 runs/day (incremental processing)       | ~$0.15–$0.30 |
| **Amazon Athena**     | ~100 lightweight queries                  | ~$0.00       |
| **Amazon S3 Storage** | Raw + curated + results (~50–60 MB total) | ~$0.001      |

**Cost Optimization Note:** The Glue ETL job runs every 4 hours (6 times/day), keeping costs at ~$0.15-0.30/month. For real-time processing, costs would increase to $1.50-3.00/month due to higher frequency.

## Future Enhancements 
- Availability heatmaps
- Peak-hour trend analytics
- Predictive “bike shortage” model