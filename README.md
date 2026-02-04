# pyspark-dataLakeHouse-project
Building A modern Data Lake House with DataBricks and Pyspark ,including ELT processes and Data Modeling
Data Lakehouse Project
Overview

This repository demonstrates a modern Data Lakehouse implementation using Databricks and Apache Spark.
It covers an end-to-end data engineering workflow—from raw data ingestion to analytics-ready datasets—following Bronze / Silver / Gold (Medallion) architecture best practices.

Project Objectives

Build a scalable Lakehouse architecture using Databricks

Ingest data from multiple source systems (ERP & CRM)

Clean, standardize, and integrate raw data

Deliver analytics-ready datasets for reporting

Apply production-oriented data engineering practices

Data Sources

ERP System – CSV files

CRM System – CSV files

Both sources are unified into a single analytical data model.

Architecture (Medallion Architecture)
🥉 Bronze Layer

Raw data ingestion

Schema enforcement & basic validations

Minimal transformations

🥈 Silver Layer

Data cleansing and standardization

Handling missing and inconsistent values

ERP & CRM data integration

🥇 Gold Layer

Business-ready datasets

Optimized data models for analytics

Designed for BI and downstream use cases

Data Quality & Processing

Duplicate removal

Handling missing and invalid values

Standardized formats and naming conventions

Latest snapshot only (no historization)

Technology Stack

Python

SQL (PostgreSQL)

PySpark

Databricks

Delta Lake

Git & GitHub

Project Scope

ERP & CRM data consolidation

Lakehouse-based data modeling

Analytics-ready outputs

No historical tracking

Documentation

Clear data model documentation

Clean, readable, and modular code

Well-organized repository structure

License

This project is licensed under the MIT License.
Free to use, modify, and distribute with proper attribution.

👋 About Me

I’m Mazen Saad Mehni, a second-year student at the Faculty of Computers and Information, focused on Data Engineering.

I completed a comprehensive Data Engineering learning path using Databricks, gaining hands-on experience in building modern Lakehouse-based data solutions.

In this project, I implemented a full Bronze / Silver / Gold pipeline covering ingestion, transformation, and analytics readiness using:

Python

SQL (PostgreSQL)

PySpark

Databricks

I was mentored by Eng. Baraa, creator of the Data with Baraa channel, who guided me through real-world use cases and industry best practices.

I’m passionate about building scalable data pipelines and transforming raw data into reliable analytical datasets.
Throughout this project, I worked with:

Python

SQL (PostgreSQL)

PySpark

Databricks

I implemented a full Bronze / Silver / Gold data pipeline, focusing on data ingestion, transformation, and analytics readiness.

I was mentored by Eng. Baraa, the creator of the “Data with Baraa” channel, who guided me through real-world use cases, industry standards, and production-oriented data engineering concepts.

I am passionate about building scalable data pipelines, transforming raw data into reliable analytical datasets, and continuously developing my skills to meet real-world data engineering challenges.

🛠️ Technical Skills

Python

SQL (PostgreSQL)

PySpark

Databricks

Data Lakehouse Architecture (Bronze / Silver / Gold)

Git & GitHub
