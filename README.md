# pyspark-dataLakeHouse-project
Building A modern Data Lake House with DataBricks and Pyspark ,including ELT processes and Data Modeling

Data Lakehouse Project
Overview

This repository presents a modern Data Lakehouse implementation built using Databricks and Apache Spark.
The project demonstrates an end-to-end data engineering workflow—from raw data ingestion to analytics-ready datasets—following industry best practices and the Bronze / Silver / Gold architecture.

The primary goal is to design a scalable and reliable data platform that enables analytical reporting and supports data-driven decision-making.

Project Objectives

Build a modern Lakehouse architecture using Databricks

Ingest data from multiple source systems (ERP & CRM)

Clean, standardize, and integrate raw data

Produce high-quality analytical datasets

Apply production-oriented data engineering practices

Data Sources

ERP System (CSV files)

CRM System (CSV files)

Both datasets are ingested and unified into a single analytical data model.

Architecture Overview

The project follows the Medallion Architecture:

🥉 Bronze Layer

Raw data ingestion from source systems

Schema enforcement and basic validations

Minimal transformations

🥈 Silver Layer

Data cleansing and standardization

Handling missing and inconsistent values

Data integration across ERP and CRM systems

🥇 Gold Layer

Business-oriented, analytics-ready datasets

Optimized data models for reporting and insights

Designed for BI and downstream analytics use cases

Data Quality & Processing

Removal of duplicates

Handling missing and invalid values

Standardization of data formats

Consistent naming conventions

Focus on the latest dataset only (no historization)

Technology Stack

Python

SQL (PostgreSQL)

PySpark

Databricks

Delta Lake

Git & GitHub

Project Scope

Consolidation of ERP and CRM data

Lakehouse-based data modeling

Analytics-ready outputs

No historical tracking (latest snapshot only)

Documentation

Clear data model documentation

Structured and readable code

Repository organized for easy navigation and review

License

This project is licensed under the MIT License.
You are free to use, modify, and distribute this project with proper attribution.

👋 About Me

My name is Mazen Saad Mehni, a second-year student at the Faculty of Computers and Information, with a strong interest in Data Engineering.

I have completed a comprehensive Data Engineering learning path using the Databricks platform, gaining hands-on experience in building modern data solutions based on Lakehouse architecture best practices.

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
